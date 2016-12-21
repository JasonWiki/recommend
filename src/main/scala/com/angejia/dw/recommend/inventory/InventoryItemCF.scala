package com.angejia.dw.recommend.inventory

import scala.collection.mutable.Map

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.angejia.dw.hadoop.hbase.HBaseClient
import com.angejia.dw.common.util.{FileUtil}

/**
 * IBCF 算法实现
   参数: 
   [zookeeperIds] [HBaseTableName] [characteristicsFile]
 */
object InventoryItemCF {

    var zookeeperIds: String = null
    var HBaseTableName: String = null
    var HBaseClientService: HBaseClient = null
    var characteristicsFile: String = null

    def main (args: Array[String]) {
        
        for (ar <- args) {
            println(ar)
        }

        // Hbase 配置
        zookeeperIds = args(0)
        HBaseTableName = args(1)
        HBaseClientService = new HBaseClient(HBaseTableName,zookeeperIds)

        // 等待训练的文件
        characteristicsFile = args(2)
        //characteristicsFile = "/data/log/recommend/recommend_user_inventory_history/00000*"
        //characteristicsFile = "/data/log/recommend/recommend_user_inventory_history/mytest2"
        //characteristicsFile = "/data/log/recommend/recommend_user_inventory_history/000000_1"

        this.calculate()
    }


    /**
     *  算法逻辑
     */
    def calculate() : Unit = {
println("----- 初始化 -----")
        val conf = new SparkConf()
        conf.setAppName("InventoryIBCF")
        conf.setMaster("local[2]")

        val sc = new SparkContext(conf)

println("----- 加载数据源: " + characteristicsFile + " -----")
        // 读取数据源
        val sourceDataRDD = sc.textFile(characteristicsFile)
        //println(sourceDataRDD.first())


println("----- 过滤数据源 -----")

        // 筛选过滤原始数据
        val filterSourceDataRDD = sourceDataRDD.filter { line =>  
            var checkStatus = true
            if (line.isEmpty()) {
                checkStatus = false
            }
            val lineArr = line.split("\t")
            if (lineArr.length < 2) {
                checkStatus = false
            } else {
                val userId = lineArr.apply(0)
                val inventoryId = lineArr.apply(1)
                if (userId.matches("[0-9]+") == false) {
                    checkStatus = false
                }
                if (inventoryId.matches("[0-9]+") == false) {
                    checkStatus = false
                } 
            }
            checkStatus
        }

println("----- 归并用户 item 集合 -----")
        // 用户喜欢 items 的集合 RDD
        val userLikeItemsCollectionRDD = filterSourceDataRDD.map { line =>
            val lineArr = line.split("\t")
            val userId = lineArr.apply(0).toInt
            val inventoryId = lineArr.apply(1).toInt
            (userId, inventoryId)
        }.groupByKey()


println("----- 用户物品集合生成矩阵 -----")
        // 用户的 Item 矩阵 B
        val userItemMatrixsRDD = userLikeItemsCollectionRDD.map{userAndItems =>
            // 用户喜欢物品的集合
            val userItems = userAndItems._2

            /**
             * 数据结构
                 Map("50:57"->1, "57:50"->1, "51:55"->1)
             */
            // 保存用户,每个物品对的矩阵 B
            val userItemMatrix : Map[String,Int] = Map[String,Int]()

            // 二二配对当前用户的物品,  为每个用户,产出一个 B 矩阵
            for (i <- userItems) {
                for (j <- userItems) { 
                    // 排除相同的物品
                    if (i != j) {
                        // 默认为每个用户 +1 个访问次数
                        //userItemMatrix += Map(i -> Map(j -> 1))
                        val key = i.toString() + ":" + j.toString()
                        userItemMatrix.put(key ,1)
                    }
                }
            }
 
            userItemMatrix
        }

        //userItemMatrixs.take(10)
        //exit()


println("----- 合并所有物品矩阵 -----")
        // 合并最终的矩阵
        val itemAndItemMatriCollection = userItemMatrixsRDD.reduce{ (x, y) =>

            var curMatrix = x
            var nextMatrix = y

            /**
             * 目标 :
             *  1. 把 curMatrix 和 nextMatrix  相同 key 的值相加
             *  2. 把 nextMatrix 不在 curMatrix 中的原样追加到 curMatrix
             */
             for ((yK, yV) <- nextMatrix) {
 
                  if (curMatrix.contains(yK) == true) {
                      curMatrix(yK) += nextMatrix(yK)
                   } else {
                      curMatrix.put(yK,yV)
                   }
             }

            curMatrix
        }
        //exit()
        //println(itemAndItemMatrix.toBuffer)


/** 把同类型的物品, 聚合到一起
(51, 51:55:2, 51:52:2, 51:53:2, 51:56:1)
(56, 56:53:1, 56:55:1, 56:52:1,56:51:1)
 */

println("----- 聚合同类型物品 -----")
        val itemAndItemGroup = itemAndItemMatriCollection.map( f => {
            val ids = f._1.split(":")
            val invetoryId = ids(0).toString()        // 房源 ID
            val invetoryRsId = ids(1).toString()      // 推荐房源 ID
            val invetoryRsIdCnt = f._2.toString()     // 共同看过的人数
            // 转换成数组
            Array(invetoryId, invetoryRsId, invetoryRsIdCnt)
            //println(invetoryId, invetoryRsId , invetoryRsIdCnt)
        // 把
        }).groupBy { 
            // 然后, 按照 invetoryId 把把同类的房源 ID groupBy 到一起
            f => f(0)
        }

        val blankLines = sc.accumulator(0)

println("----- 把聚合后的数据格式化成字符串 -----")
        val itemAndItemGroupToStringRDD = itemAndItemGroup.map(line => {
            val invetoryId = line._1
            val invetoryRsInfo = line._2
            // 把里面的 array 按照:组合, 最外层按照,组合
            val invetoryRsToString = invetoryRsInfo.map(f => f.mkString(":")).mkString(",")

            blankLines += 1
            // println("write[" + blankLines + "]: " + invetoryId)
            //结果写到 Hbase (invetoryId, invetoryRsToString)
            resultWriteHBase(invetoryId, invetoryRsToString)
        })

        println("")
        println("-----  HBase Table inventoryRecommend: ",
                " inventoryRecommend 写入了: " + blankLines + " 行")
        
        //itemAndItemGroupToStringRDD
/*
       //val blankLines = sc.accumulator(0)
println("----- 写到 Hbase -----")
        var n = 0
        itemAndItemGroupToStringRDD.foreach(f => {
            n += 1
            println("write[" + n + "]: " + f._1)
            resultWriteHBase(f._1, f._2)
        })

    
*/
    }
    
    // 拆解 Map 返回
    def dismantlingMap (data: Map[Int,Int]): (Int, Int) = {
        val keys = data.keySet.toArray

        (keys(0), data.get(keys(0)).get)
    } 


    def resultWriteHBase(rowKey: String, value: String) : Unit = {
        //FileUtil.fileOutputStream("/data/log/recommend/result",rowKey + "--" + value + "\n",true)
        HBaseClientService.insert(rowKey, "inventoryRecommendInventory", "inventoryIds", value)
    }


}