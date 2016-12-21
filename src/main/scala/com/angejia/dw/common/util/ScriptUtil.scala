package com.angejia.dw.common.util

import sys.process._
import java.io.BufferedReader
import java.io.InputStreamReader

object ScriptUtil {


    def runSyncCommand(cmd: String): Map[String,Any] = {
       val qb = Process(cmd)

       var out = ""
       var err = ""
 
       val exitCode = qb ! ProcessLogger( 
              // 匿名函数
              (s) => {
                  out += s + "\n"
               }, 
              (s) => {
                  err += s + "\n"
              }
       )

       val result: Map[String,Any] = Map(
            "code" -> exitCode, 
            "stdoutPut" -> out,
            "erroutPut" -> err
       )

        result
    }



    /** Run a command, collecting the stdout, stderr and exit status */
    def runCommandBak(in: String): (List[String], List[String], Int) = {
      val qb = Process(in)
      var out = List[String]()
      var err = List[String]()
 
      val exit = qb ! ProcessLogger(
              (s) => out =  List(s), 
              (s) => err ::= s)

      (out.reverse, err.reverse, exit)
    }
}