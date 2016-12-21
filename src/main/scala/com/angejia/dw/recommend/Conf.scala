package com.angejia.dw.recommend

import scala.collection.mutable.Map
import scala.io.Source
import java.io.{ InputStream, BufferedReader, InputStreamReader, PushbackReader }

import com.angejia.dw.common.util.PropertyUtil

object Conf {

    // 读取配置文件
    val property = new PropertyUtil()

    // 项目根目录
    //var projectPath = Conf.getClass().getResource("/").getFile().toString()

    /**
     * 设置环境,根据不同的环境使用不同的配置文件
     */
    def setEnv(env: String = "dev") : Unit = {

        // 读取的配置文件名称
        val confName = "/conf_" + env + ".properties"

        /**
         * 读取 resource 目录下的文件
         * val lines = Source.fromURL(getClass.getResource(confName)).getLines()
           lines.foreach(println)
         */
        // 获取 resource 文件读输入流
        val inputStreamReader: InputStreamReader = Source.fromURL(getClass.getResource(confName)).reader()

        // 设置读取的流
        property.setFileInputStream(inputStreamReader)
    }


    /**
     * 获取 zookeeper 地址
     */
    def getZookeeperQuorum(): String = {
        val zookeeperQuorum: String = property.getKeyValue("zookeeperQuorum") 
        zookeeperQuorum
    }


    /**
     * 获取 kafka 服务器地址
     */
    def getKafkaServerBrokerList() : String = {
        val kafkaServerBrokerList: String = property.getKeyValue("kafkaServerBrokerList")
        kafkaServerBrokerList
    }


    /**
     * 获取业务数据库配置信息
     */
    def getProductMysqDBInfo(): Map[String,String] = {
        Map[String, String](
           "host" ->  property.getKeyValue("productMysqlDB.host"),
           "account" -> property.getKeyValue("productMysqlDB.account"),
           "password" -> property.getKeyValue("productMysqlDB.password"),
           "defaultDB" -> property.getKeyValue("productMysqlDB.defaultDB")
        )
    }


    /**
     * 获取bi数据库配置信息
     */
    def getBiMysqDBInfo(): Map[String,String] = {
        Map[String, String](
           "host" ->  property.getKeyValue("biMysqlDB.host"),
           "account" -> property.getKeyValue("biMysqlDB.account"),
           "password" -> property.getKeyValue("biMysqlDB.password"),
           "defaultDB" -> property.getKeyValue("biMysqlDB.defaultDB")
        )
    }


    /**
     * hdfs 文件服务地址
     */
    def getHDFSServer(): String = {
        val hdfsServer: String = property.getKeyValue("HDFSServer")
        hdfsServer
    }


    /**
     * 获取 hive 相关配置信息
     */
    def getHiveConf(): Map[String,String] = {
        Map[String, String](
           "hiveMetastoreUris" ->  property.getKeyValue("hive.metastore.uris"),
           "hiveThriftServerUrl" ->  property.getKeyValue("hive.thrift.server.url"),
           "hiveThriftServerUser" ->  property.getKeyValue("hive.thrift.server.user"),
           "hiveThriftServerPass" ->  property.getKeyValue("hive.thrift.server.pass")
        )
    }


    /**
     * 获取 spark 相关配置信息
     */
    def getSparkConf(): Map[String,String] = {
        Map[String, String](
           "sparkThriftServerUrl" ->  property.getKeyValue("spark.thrift.server.url"),
           "sparkThriftServerUser" ->  property.getKeyValue("spark.thrift.server.user"),
           "sparkThriftServerPass" ->  property.getKeyValue("spark.thrift.server.pass")
        )
    }
}