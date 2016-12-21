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

/**
 * 操作 HDFS 类
 */
class HDFSClient(conf: Configuration)  {

//Initial Configuration

  //private var conf = new Configuration()
  //private var maprfsCoreSitePath = new Path("core-site.xml")
  //private var maprfsSitePath = new Path("maprfs-site.xml")

  //conf.addResource(maprfsCoreSitePath)
  //conf.addResource(maprfsSitePath)

    private var fileSystem = FileSystem.get(conf)

    /**
     * 创建 HDFS 目录
     */
    def mkdirs(hdfsFolderPath: String): Unit = {
       var path = new Path(hdfsFolderPath)
         if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path)
         }
    }


    /**
     * 创建一个空的 HDFS 文件
     */
    def createNewFile(hdfsFilePath:String): Path = {
        val path = new Path(hdfsFilePath)
        if (!fileSystem.exists(path)) {
            var out = fileSystem.createNewFile(path)
        }
        path
    }


    /**
     * 创建或者修改 HDFS 文件
     */
    def createAndSave(hdfsPath: String): Unit = {
        var out = fileSystem.create(new Path(hdfsPath))
        var in = new BufferedInputStream(new FileInputStream(hdfsPath))
        var b = new Array[Byte](1024)
        var numBytes = in.read(b)
        while (numBytes > 0) {
          out.write(b, 0, numBytes)
          numBytes = in.read(b)
        }
        in.close()
        out.close()
    }


    /**
     * 追加本地文件到 HDFS 文件中
     */
    def appendFileToHdfsFile(fromfilepath: String, hdfsFilePath: String): Unit = {
       val hdfsPath = this.createNewFile(hdfsFilePath)
       // hfds
       var out = fileSystem.append(hdfsPath)

       // 本地文件流
       var in = new BufferedInputStream(new FileInputStream(new File(fromfilepath)))
       var b = new Array[Byte](1024)
       var numBytes = in.read(b)
       while (numBytes > 0) {
         out.write(b, 0, numBytes)
         numBytes = in.read(b)
        }
        in.close()
        out.close()
    }


    /**
     * 追加数据到 HFDS 中
     */
    def appendDataToHdfsFile(data: String, hdfsFilePath: String) : Unit = {
        val path = this.createNewFile(hdfsFilePath)

        var out = fileSystem.append(new Path(hdfsFilePath))

        val by = data.getBytes()
        out.write(by, 0, by.length)
        out.close()
    }


    /**
     * 读取 HDFS 文件流
     */
    def getFile(hdfsPath: String): InputStream = {
        var path = new Path(hdfsPath)
        fileSystem.open(path)
        
    }


    /**
     * 删除 hdfs 文件
     */
    def deleteFile(hdfsPath: String): Boolean = {
        var path = new Path(hdfsPath)
        fileSystem.delete(path, true)
    }


    /**
     * Close the FileSystem Handle
     */
    def close() = {
         fileSystem.close
    }
 }