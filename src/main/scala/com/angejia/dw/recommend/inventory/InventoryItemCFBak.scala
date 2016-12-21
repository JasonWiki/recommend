package com.angejia.dw.recommend.inventory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
import scala.collection.mutable.LinkedList
import scala.collection.mutable.ArrayBuffer

import com.angejia.dw.hadoop.hbase.HBaseClient
import com.angejia.dw.common.util.{FileUtil}


object InventoryItemCFBak {

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
        //HBaseClientService = new HBaseClient(HBaseTableName,zookeeperIds)

        // 等待训练的文件
        characteristicsFile = args(2)
        characteristicsFile = "/data/log/recommend/recommend_user_inventory_history/00000*"
        //characteristicsFile = "/data/log/recommend/recommend_user_inventory_history/mytest2"
        
        this.calculate()
    }
    
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


println("----- 归并用户 item 集合 -----")
        // 用户喜欢 items 的集合 RDD
        val userLikeItemsCollectionRDD = sourceDataRDD.map(line => {
            val curLine = line.split("\t").map { x => x.toInt}
            //println(curLine(0) + " " + curLine(1) + " " + curLine(2))
            (curLine(0), curLine(1))
        }).groupByKey()


println("----- 用户物品集合生成矩阵 -----")
        // 用户的 Item 矩阵 B
        val userItemMatrixs = userLikeItemsCollectionRDD.map{userAndItems =>
            val userItems = userAndItems._2

            /*
            val map : Map[Int,Map[Int,Int]] = Map[Int,Map[Int,Int]]()
            var am : Map[Int,Int] = Map[Int,Int]()
            val a = Map(1 -> 2)
            map.put(1,a)
            println(map(1)(1))
            * */

            // 保存用户,每个物品对的矩阵 B
            //var itemMatrix = ArrayBuffer[ArrayBuffer[Int]]()
            //var itemMatrix = Array.ofDim[Int](55,55)
            var userItemMatrix : Map[Int,Map[Int,Int]] = Map[Int,Map[Int,Int]]()

            // 二二配对当前用户的物品,  为每个用户,产出一个 B 矩阵
            for (i <- userItems) {
                for (j <- userItems) {
                    // 排除相同的物品
                    if (i != j) {
                        // 默认为每个用户 +1 个访问次数
                        userItemMatrix.put(i,Map(j -> 1))
                    }
                }
            }
            userItemMatrix
        }


println("----- 合并用户矩阵 -----")

        // 合并最终的矩阵
        val itemAndItemMatrix = userItemMatrixs.reduce{ (x, y) =>
            // var curRsMatrix : Map[Int,Map[Int,Int]] = Map[Int,Map[Int,Int]]()

            var curMatrix = x
            var nextMatrix = y

            // 合并 2 个 map ,把用户矩阵相加
            for ((yK, yV) <- nextMatrix) {
               val iKey = yK
               val tmp = yV.keySet.toArray
               val jKey = tmp(0).toInt

               // 当前 x 的 map key 和 // 子 map 的 key 相同
               if (x.contains(iKey) == true && x.get(iKey).get.contains(jKey) == true) {
                  curMatrix(iKey)(jKey) += 1
               } else {
                   // 追加矩阵
                   curMatrix.put(iKey,yV)
               }
               //println("yK:" + yK + "  yV:" + yV)
            }

            //println("-----")
            curMatrix
        }

        //println(itemAndItemMatrix.toBuffer)

println("----- 写到 Hbase -----")
        itemAndItemMatrix.foreach{ f =>
            val invetoryId = f._1
            val invetoryRsInfo = this.dismantlingMap(f._2)
            val invetoryRsId = invetoryRsInfo._1
            val invetoryRsIdCnt = invetoryRsInfo._2

            println(f)
            println(invetoryId , invetoryRsId , invetoryRsIdCnt)
            println("-----")
            val key = invetoryId.toString() + ":"
            val value = invetoryRsId.toString() + ":" + invetoryRsIdCnt.toString()
            this.resultWriteHBase(key,value)
            //this.resultWriteHBase()
            
        }

        //itemAndItemMatrix.foreach(f => println(f))
        //itemAndItemMatrix.take(10)


    }


    // 拆解 Map 返回
    def dismantlingMap (data: Map[Int,Int]): (Int, Int) = {
        val keys = data.keySet.toArray

        (keys(0), data.get(keys(0)).get)
    } 


    def resultWriteHBase(rowKey: String, value: String) : Unit = {
        //FileUtil.fileOutputStream("/data/log/recommend/result","",false)
        FileUtil.fileOutputStream("/data/log/recommend/result",rowKey + value + "\n",true)
        //HBaseClientService.insert(rowKey, "inventoryRecommendInventory", "inventoryIds", value)
    }


}