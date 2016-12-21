package com.angejia.dw.recommend.inventory

import scala.collection.mutable.Map

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.angejia.dw.recommend.Conf
import com.angejia.dw.hadoop.hbase.HBaseClient
import com.angejia.dw.recommend.IBCF
import com.angejia.dw.common.util.{FileUtil}

/**
 * IBCF 算法实现
 * create 'inventoryIBCF',{NAME=>'baseInfo'},{NAME=>'recommend'}
 */
object InventoryIBCF {

    // hbase 数据表
    var hbaseResultTb: HBaseClient = null

    // 等待训练的数据文件
    var characteristicsFile: String = null

    // 文件分隔符
    var separator = "\t"

    def main (args: Array[String]) {

        for (ar <- args) {
            println(ar)
        }

        val env = args(0)    
        this.init(env)

        this.characteristicsFile = args(1)
        //this.separator = " "
        //this.characteristicsFile = "/data/log/recommend/recommend_user_inventory_history/mytest"

        this.calculate()

    }


    /**
     * 初始化
     * env:  dev 开发环境， online 线上环境
     */
    def init (env: String): Unit =  {
        Conf.setEnv(env)

        println(Conf.getZookeeperQuorum())
        // 连接 userUBCF 数据表
        this.hbaseResultTb = new HBaseClient("inventoryIBCF",Conf.getZookeeperQuorum())
        println(Conf.getZookeeperQuorum())
    }


    def calculate(): Unit = {

        /**
         * 初始化 spark 
         */
        val sparkConf = new SparkConf()
        sparkConf.setAppName("InventoryIBCF")
        sparkConf.setMaster("local[2]")

        /**
         * 初始化推荐模型
         */
        val inventoryIBCF = new IBCF()

        // 合并累加 ItemAndItemMatrix 矩阵
        val itemAndItemMatrixCollection = inventoryIBCF.calculateByFile(sparkConf, characteristicsFile, separator)

        // 根据 ItemId , groupBy ItemMatrix
        val itemAndItemGroup = inventoryIBCF.itemMatrixGroupByItemId(itemAndItemMatrixCollection)

        var inventoryLine = 0         // 推荐行数
println("----- 把聚合后的数据格式化成字符串保存在 Hbase -----")
        itemAndItemGroup.foreach(line => {
            val invetoryId = line._1
            val invetoryRsInfo = line._2

            // 把里面的 array 按照:组合, 最外层按照,组合
            val invetoryRsToString = invetoryRsInfo.map(f => f.mkString(":")).mkString(",")

            this.inventoryRecommendWriteHbase(invetoryId, invetoryRsToString)

            inventoryLine += 1
        })

        println("")
        println("-----  HBase Table inventoryIBCF: ",
                "写入了: " + inventoryLine + " 行")

    }


    /**
     * 保存推荐结果到 Hbase
     */
    def inventoryRecommendWriteHbase(rowKey: String, value: String): Unit = {
        this.hbaseResultTb.insert(rowKey, "recommend", "inventoryRecommend", value)
    }



}