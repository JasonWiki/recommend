package com.angejia.dw.common.util

import scala.util.matching.Regex


object RegexUtil {


    def findStrData(pattern: String, strData: String) : String = {
        var rs = ""

        // String 类的 r() 方法构造了一个Regex对象
        val patternObj: Regex = pattern.r

        // findFirstIn 方法找到首个匹配项
        val findRs: Option[String] = patternObj.findFirstIn(strData)

        if (!findRs.isEmpty) {
            //val patternObj(num,str) = string
            //rs = args

            // 模式匹配, 当前字符串匹配到了正则
            strData match{  
                case patternObj(str) => 
                    //println(str)
                    rs = str
                case _=> 
                    println("Not matched")  
            }
        }
        rs
    }


    def findStrDataBak(pattern: String, string: String) : String = {
        var rs = ""
        // 使用 Regex 构造对象
        val patternObj = new Regex(pattern)  // 首字母可以是大写 S 或小写 s
        println(patternObj.findFirstIn(string))

        println(patternObj findFirstIn string) 
        rs
    }


    def test() : Unit = {
        
        // 构造 Regex 对象, 用 String 类的 r 方法即可
         val pattern = "Scala".r
         val str = "Scala is Scalable and cool"
         println(pattern findFirstIn str) //  println(pattern.findFirstIn(str))

         // 使用 Regex 构造对象
         val pattern2 = new Regex("(S|s)cala")  // 首字母可以是大写 S 或小写 s
         val str2 = "Scala is scalable and cool"
         println(pattern2 findFirstIn str2) 
         
         
         val filter_regex="/mobile/member/inventories/list[?](.*)".r
         val str3 = "/mobile/member/inventories/list?bedroom_id=2&city_id=1&sort_id=3&price_id=4&district_id=7&block_id=62&page=1&per_page=8"
         if (!filter_regex.findFirstIn(str3).isEmpty) {
             val filter_regex(pars) = str3
             println(pars)
         }

    }
}