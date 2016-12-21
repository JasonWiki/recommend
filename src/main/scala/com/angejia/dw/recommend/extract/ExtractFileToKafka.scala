package com.angejia.dw.recommend.extract

import java.lang.{Runtime,Thread}
import com.angejia.dw.common.util.{ListenerFile,ScFileUtil,FileUtil}
import com.angejia.dw.hadoop.kafka.{KafkaProducer,KafkaConsumer}

/**
 * 动态抽取日志到 kafka
 */

object ExtractFileToKafka {

    def main(args: Array[String])  {
        val zookeeperConnect = args(0)
        val kafkaBrokerList =  args(1)
        val kafkaTopic = args(2)
        val kafkaTopicPartition = args(3)
        val kafkaConsumerGroupId = args(4)
        val listenerConfFile = args(5)
        val stepLength = args(6)

        val extractAccessLog = new ExtractFileToKafka
        extractAccessLog.zookeeperConnect = zookeeperConnect
        extractAccessLog.kafkaBrokerList = kafkaBrokerList
        extractAccessLog.kafkaTopic = kafkaTopic
        extractAccessLog.kafkaTopicPartition = kafkaTopicPartition
        extractAccessLog.kafkaConsumerGroupId = kafkaConsumerGroupId
        extractAccessLog.listenerConfFile = listenerConfFile
        extractAccessLog.stepLength = stepLength
        //extractAccessLog.stepLength = 1000.toString()
        extractAccessLog.runExtractAccessLog()
    }

    def runTest(): Unit = {
       val extractAccessLog = new ExtractFileToKafka
       extractAccessLog.zookeeperConnect = "dwtest:2181"
       extractAccessLog.kafkaBrokerList = "dwtest:9092"
       extractAccessLog.kafkaTopic = "accessLog"
       extractAccessLog.kafkaTopicPartition = "0"
       extractAccessLog.kafkaConsumerGroupId = "userPortrait"
       extractAccessLog.listenerConfFile = "/data/log/recommend/accesslog"
       extractAccessLog.runExtractAccessLog()
    }
}

/**
 * 等待完成功能
 * 1. 程序退出、失败，记录最后一次更新点
 * 2. 可以读取最后一次更新的文件,作为标记开始的日期
 * 3. 当目标文件不存在(等待)
 */
class ExtractFileToKafka {

    // zookeeper 服务器
    var zookeeperConnect : String = null

    // kafka broker 服务器
    var kafkaBrokerList : String = null

    // kafka Topic
    var kafkaTopic : String = null

    // kafka Topic Partition
    var kafkaTopicPartition : String = null

    // kafka Consumer GroupId
    var kafkaConsumerGroupId : String = null

    // kafkaProducer 连接对象
    var kafkaProducer: KafkaProducer = null

    // kafkaConsumer 连接对象
    var kafkaConsumer: KafkaConsumer = null

    // 监听的配置文件
    var listenerConfFile: String = null

    // 每次读取行的长度
    var stepLength: String = null


    /**
     * 监听日志文件,并且发送日志到 kafka 中
     */
    def runExtractAccessLog(): Unit = {
        // 读取配置文件
        val readFileConf = ScFileUtil.fileInputStream(this.listenerConfFile)
        val readFileConfArgs = readFileConf.split("\\s+")
        val lsFile = readFileConfArgs(0)         // 监听文件
        val lsFileDate = readFileConfArgs(1)     // 监听文件的日期
        val lsFileLineNum: String = readFileConfArgs(2)     // 监听文件读到的行数

        val listenerFile = new ListenerFile() 

        //listenerFile.listenerDateFile(lsFile, lsFileDate, lsFileLineNum.toInt, stepLength.toInt, this.readLogLine)
        listenerFile.listenerDateFileWhile(lsFile, lsFileDate, lsFileLineNum.toInt, stepLength.toInt ,this.readLogLine)
    }



    /**
     * 回调函数,发送日志到 Kafka
     */
    def readLogLine(result: Map[String,Any]): Unit = {
        val file = result.get("file").get.toString()    // 当前读的到的文件
        val fileTemplate = result.get("fileTemplate").get.toString() // 文件模板
        val date = result.get("date").get.toString() // 当前读到的文件日期
        val nextLineNum = result.get("nextLineNum").get.toString()     // 下一次开始读取的行数
        val readLineCmd = result.get("readLineCmd").get.toString()     // 当前读到的行数
        val fileLineContent = result.get("fileLineContent").get.toString()    // 当前读到的文件内容

        val curLog = "NextReadFile: " + file + " " + date + " " + nextLineNum
        println(readLineCmd)
        println(curLog)
        //println(fileLineContent)

        if (fileLineContent.length() != 0) {
            // 发送日志到 kafka
            this.producerToKafka(fileLineContent)

            // 文件记录点
            val filePoint = fileTemplate + " " + date + " " + nextLineNum
            FileUtil.fileOutputStream(this.listenerConfFile, filePoint, false)

            // 记录一份到 本地日志 : 调试用
            //val localFile = "/var/log/ExtractFileToKafka/ExtractFileToKafka_" + date + ".log"
            //FileUtil.fileOutputStream(localFile, fileLineContent, true)

            println("filePoint: " + filePoint)
        }

        println("----------")

    }


    /**
     *  获取 KafkaProducer 连接对象
     */
    def getKafkaProducerObj(): KafkaProducer = {
        if (this.kafkaProducer == null ) {
            this.kafkaProducer = new KafkaProducer(this.kafkaTopic,this.kafkaBrokerList)
        }
        this.kafkaProducer 
    }


    /**
     * 获取 KafkaConsumer 连接对象
     */
    def getConsumerKafka(): KafkaConsumer = {
        if (this.kafkaConsumer == null) {
            this.kafkaConsumer = new KafkaConsumer(this.kafkaTopic,this.kafkaConsumerGroupId,this.zookeeperConnect)
        }
        this.kafkaConsumer
    }


    /**
     * 发送数据给 Kafka
     */
    def producerToKafka(content: String): Unit = {
        this.getKafkaProducerObj().send(content,this.kafkaTopicPartition)
    }


    
    /**
     * 消费数据
     */
    def consumerKafka () : Unit = {
        this.getConsumerKafka().read(this.consumerData)
    }



    /**
     * 回调函数
     */
    def consumerData(a: Array[Byte]): Unit = {
        println(a(0).toBinaryString)
        println(123) 
    }

}


  


