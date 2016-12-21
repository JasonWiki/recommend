package com.angejia.dw.hadoop.hdfs

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileUtil

import org.apache.hadoop.conf.Configuration
import com.angejia.dw.hadoop.hdfs.HDFSClient

object HDFSClientTest  {


    def main(args: Array[String])  {
        val ob = new HDFSClientTest
        ob.run(args(0), args(1))
    }
}


class HDFSClientTest {

    def run(hdfsPath: String, localPath: String) : Unit = {
        //fs.defaultFS 8020
        val conf = new Configuration()
        conf.set("fs.defaultFS", "hdfs://uhadoop-ociicy-master1:8020") // 写地址
        conf.setBoolean("dfs.support.append", true)    // 开启追加模式


        val hdfsServer = new HDFSClient(conf)
        // 创建文件
        //hdfsServer.createNewFile("/user/hive/real_time/source_data/access_log/aaa.txt")
        // 追加数据
        //hdfsServer.appendFileToHdfsFile("/data/tmp/test.log","/user/hive/real_time/source_data/access_log/aaa.txt")
        hdfsServer.appendDataToHdfsFile("你好呀",hdfsPath)
        hdfsServer.appendDataToHdfsFile("你好呀",hdfsPath)
        //hdfsServer.appendDataToHdfsFile("你好呀",hdfsPath)
        //hdfsServer.appendDataToHdfsFile("你好呀",hdfsPath)
        // 读取内容
        val content = hdfsServer.getFile(hdfsPath)
        println(content)
    }
}


 



