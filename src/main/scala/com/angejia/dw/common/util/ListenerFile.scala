package com.angejia.dw.common.util

import java.util.concurrent.TimeUnit

class ListenerFile {
 
    /**
     * 读取小文件可以,但是针对大文件,会有部分问题 
     *   Exception in thread "main" java.lang.StackOverflowError
     *   调整 java -Xss512M
     * 从指定文件日期开始,监听文件的变化,并发送给回调函数
     * file : 需要监听文件路径,格式: /data/log/uba/lb/access.${date}.log
     * date : 指定文件日期(天) , 格式: 20160101
     * lineNum : 从多少行开始读
     * stepLength : 每次读取多少行, 如果每次读取 1 行写 0, 如果每次读取 100 行写 100
     * 
     * callback : 回调函数
     * 
     
     def readLogLine(result: Map[String,Any]): Unit = {
         println(result)
     }
     */
    def listenerDateFile(
            file: String, 
            date: String, 
            lineNum: Int, 
            stepLength: Int,
            callback: Map[String,Any] => Unit,    // 回调函数
            isRecursive: Boolean = true // 是否递归调用
    ): Map[String,Any] = {

        // 当前运行日期
        var curDate = date

        // 当前读的文件
        val curReadFile = file.replace("${date}", date)

        // 等待执行 sh 命令 sed -n '5,7p'
        val startLine = lineNum
        val endLine = lineNum + stepLength - 1
        var readLineCmd = "sed -n " + startLine + "," + endLine + "p " + curReadFile
        val commandResult = ScriptUtil.runSyncCommand(readLineCmd)       // 执行返回结果
        val commandCode = commandResult.get("code").get                  // 执行状态
        val curLine = commandResult.get("stdoutPut").get.toString()      // 获取标准输出


        // 当前定位行数
        var curLineNum: Int = 0

        // 结果数据的行数
        val curLineResult = curLine.split("\n")
        val curLineResultLength = curLineResult.length // 一共读了多少行

        // 当行数分解的数组长度为 1， 并且第一元素的值为空 , 表示读取的数据为空了
        val rs = curLineResultLength <= 1 && curLineResult(0).length() == 0 // 为空是 true , 不为空是 false

        // 读取的行数为空, 日期是今天的 
        if (rs == true && date == this.getCurDate()) {
            // 把位置定位到开始的时间
            curLineNum = startLine

            // 等待 3 秒后再执行
            TimeUnit.SECONDS.sleep(3); 

        // 读取的行数为空, 日期不是当天的日期
        } else if (rs == true && date != this.getCurDate()) {
            // 当前日期增加 1 天，tomorrowDate
            curDate = this.getOffsetDate(1,date)

            // 文件从第一行开始读
            curLineNum = 1

        // 读取的行数不为空, 不是今天日期, 也不是隔天日期, 表示是正常累加的行数
        } else {
            if (curLineResultLength < stepLength) {
               TimeUnit.SECONDS.sleep(3);
            }
          
            // 则把行数定位到 开始行数 + 总共读取的行数
            curLineNum = startLine + curLineResultLength
        }


        // 返回的结果
        var result = Map(
            // 读到的文件
            "file" -> curReadFile,
            // 下一次开始读的行数
            "nextLineNum" -> curLineNum,
            // 读到的行内容
            "fileLineContent" -> curLine,
            // 文件模板
            "fileTemplate" -> file,
             // 日期
            "date" -> curDate,
            // 读到的命令
            "readLineCmd" -> readLineCmd,
            // 命令返回的参数
            "commandResult" -> commandResult
        )
        callback(result) // 回调函数
 
        if (isRecursive == true) {
            // 递归,从指定日期第 n 行开始读取数据
            this.listenerDateFile(file, curDate, curLineNum, stepLength,callback)
        }
        result

    }


     /**
     * While 方式监听文件变化
     * file : /data/log/uba/lb/access.${date}.log  监听的文件
     * date : 20160101 日期
     * lineNum: 行数
     * stepLength : 步长
     */
    def listenerDateFileWhile(
            file: String, 
            date: String, 
            lineNum: Int, 
            stepLength: Int,
            callback: Map[String,Any] => Unit    // 回调函数
    ) : Unit = {
        var status = true

        // 当前运行时间
        var curDate =  date

        // 当前读到的行数
        var curLineNum = lineNum

        var map = Map[String,Any]();
        while( status ){
            map = this.listenerDateFile(file, curDate, curLineNum, stepLength, callback, false)
            curDate = map.get("date").get.toString()
            curLineNum = map.get("nextLineNum").get.toString().toInt
        }

    }


    // 日期增加 减少 1 天
    def getOffsetDate(offset: Int, dateStr: String): String = {
        // 字符日期转换为 Date 对象
        val date = DateUtil.StringToDate(dateStr, DateUtil.SIMPLE_YMD_FORMAT)
        // date 对象 + n 天
        val offsetDate = DateUtil.getCalendarOffsetDateDay(offset, date);
        // date 对象转换成 str
        DateUtil.DateToString(offsetDate, DateUtil.SIMPLE_YMD_FORMAT)
    } 


    // 获取当前系统时间
    def getCurDate(): String = DateUtil.TimestampToSting(DateUtil.getNowTimestamp,DateUtil.SIMPLE_YMD_FORMAT)
}