package com.angejia.dw.common.util

import scala.io.Source

object ScFileUtil {
   
    /**
     * 读取文件
     * val readFile = fileInputStream.split("\\s+")
     */
    def fileInputStream(fileName: String,encoded: String = "UTF-8"): String = {
        val source = Source.fromFile(fileName,encoded)
        val contents = source.mkString.toString()
        contents
    }
    
    
    
    
}