package com.angejia.dw.recommend

import scala.collection.mutable.Map

import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * 基于 spark Rdd 实现 IBCF 算法
 */
class IBCF extends Serializable  {

    /**
     * 模型的数据源文件 : HDFS 或者 普通文件
     */
    var characteristicsFile: String = null
    def setCharacteristicsFile(characteristicsFile: String): Unit = {
        this.characteristicsFile = characteristicsFile
    }
    def getCharacteristicsFile(): String = {
        this.characteristicsFile
    }


    /**
     * 文件行分隔符
     */
    var fileSeparator: String = "\t"
    def setFileSeparator(fileSeparator: String): Unit = {
        this.fileSeparator = fileSeparator
    }
    def getFileSeparator(): String = {
        this.fileSeparator
    }


    /**
     * 根据 SparkConf 初始化 SparkContext 上下文
     */
    var sparkContext: SparkContext = null
    def setSparkContext(sparkConf: SparkConf): Unit = {
        this.sparkContext = new SparkContext(sparkConf)
    }
    def getSparkContext(): SparkContext  = {
        this.sparkContext
    }


    /**
     * 计算矩阵并得到 item -> item 关系矩阵, 根据输入文件或者 HDFS
     */
    def calculateByFile(sparkConf: SparkConf, characteristicsFile: String, fileSeparator: String): Map[String, Int] = {

println("----- 初始化 sparkContext 上下文 -----")
        this.setSparkContext(sparkConf)

        // 设置加载文件
        this.setCharacteristicsFile(characteristicsFile)
        this.setFileSeparator(fileSeparator)

        // 加载数据, 生成 RDD 
        val baseModelRDD = this.loadModelData(this.getCharacteristicsFile())

        // 过滤模型
        val modelRDD =  this.filterModelRDD(baseModelRDD, this.getFileSeparator())

        // 生成 user -> item 矩阵
        val itemAndItemMatrixRDD = this.generateItemAndItemMatrix(modelRDD)

        // 合并所有 user -> item 矩阵
        val itemAndItemMatrixCollection = this.mergeItemAndItemMatrix(itemAndItemMatrixRDD)

        itemAndItemMatrixCollection
    }
    
    
    
    /**
     * 计算矩阵并得到 item -> item 关系矩阵 - 根据基础输入模型
     */
    def calculateByBaseModelRDD(baseModelRDD: RDD[String], fileSeparator: String): Map[String, Int] = {

        this.setFileSeparator(fileSeparator)

        // 过滤模型
        val modelRDD =  this.filterModelRDD(baseModelRDD, this.getFileSeparator())

        // 生成 user -> item 矩阵
        val itemAndItemMatrixRDD = this.generateItemAndItemMatrix(modelRDD)

        // 合并所有 user -> item 矩阵
        val itemAndItemMatrixCollection = this.mergeItemAndItemMatrix(itemAndItemMatrixRDD)

        itemAndItemMatrixCollection
    }



    /**
     * 加载模型数据
     * 
     */
    def loadModelData(characteristicsFile: String) : RDD[String] = {

println("----- 加载模型数据: " + characteristicsFile + "-----")
        // 读取数据源
        val baseModelRDD = this.getSparkContext().textFile(characteristicsFile)

        baseModelRDD
    }


    /**
     * 过滤 RDD 模型
     * modelRDD: 根据数据源生成基础 RDD
     * fileSeparator : 文件行分隔符
     */
    def filterModelRDD(modelRDD: RDD[String], fileSeparator: String): RDD[String] = {
 
println("----- 过滤模型非法数据 -----")

        // 筛选过滤原始数据
        val filterRDD = modelRDD.filter { line =>  
            var checkStatus = true
            if (line.isEmpty()) {
                checkStatus = false
            }
            val lineArr = line.split(fileSeparator)
            if (lineArr.length < 2) {
                checkStatus = false
            } else {
                val userId = lineArr.apply(0)
                val itemId = lineArr.apply(1)
                //if (userId.matches("[0-9]+") == false) {
                    //checkStatus = false
                //}
                //if (itemId.matches("[0-9]+") == false) {
                   // checkStatus = false
                //} 
                if (userId == "" || itemId == "") {
                    checkStatus = false
                }
            }
            checkStatus
        }

        filterRDD
    }



    /**
     * 根据 user 集合, 生成 item -> item 矩阵
     * modelRDD : 基础数据模型
     * return RDD(
     *     Map("50:57"->1, "57:50"->1, "51:55"->1),
     *     Map("50:57"->2, "57:50"->3, "51:55"->4)
     * )
     */
    def generateItemAndItemMatrix(modelRDD:  RDD[String]) :  RDD[Map[String, Int]] = {

println("----- 归并 user -> items 集合 -----")
        // 用户喜欢 items 的集合 RDD
        val userLikeItemsCollectionRDD = modelRDD.map { line =>
            val lineArr = line.split("\t")
            val userId = lineArr.apply(0).toString()
            val itemId = lineArr.apply(1).toString()
            (userId, itemId)
        }.groupByKey()

println("----- 根据 user -> items 集合, 生成 item -> item 矩阵 -----")
        // 用户的 Item 矩阵 B
        val itemAndItemMatrixRDD = userLikeItemsCollectionRDD.map{userItemsCollection =>
            // 用户喜欢物品的集合
            val userItems = userItemsCollection._2

            /**
             * 数据结构
                 Map("50:57"->1, "57:50"->1, "51:55"->1)
             */
            // 保存用户,每个物品对的矩阵 B
            val itemAndItemMatrix : Map[String,Int] = Map[String,Int]()

            // 二二配对当前用户的物品,  为每个用户,产出一个 B 矩阵
            for (i <- userItems) {
                for (j <- userItems) { 
                    // 排除相同的物品
                    if (i != j) {
                        // 默认为每个用户 +1 个访问次数
                        //userItemMatrix += Map(i -> Map(j -> 1))
                        val key = i.toString() + ":" + j.toString()
                        itemAndItemMatrix.put(key ,1)
                    }
                }
            }
 
            itemAndItemMatrix
        }

        itemAndItemMatrixRDD
    }



    /**
     *  合并累加 ItemAndItemMatrix 矩阵
     *  itemAndItemMatrix : item -> item 矩阵
     */
    def mergeItemAndItemMatrix(itemAndItemMatrix: RDD[Map[String, Int]]): Map[String, Int] = {
println("----- 合并累加所有 item -> item  矩阵 -----")

        var rsItemMatrix: Map[String, Int] = Map[String, Int]()

        if (itemAndItemMatrix.count() == 0) {
           return rsItemMatrix
        }

        // 合并最终的矩阵
        val itemAndItemMatrixCollection = itemAndItemMatrix.reduce{ (x, y) =>

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

        itemAndItemMatrixCollection
    }


    /**
     * 根据 ItemId , groupBy ItemMatrix
      把同类型的物品, 聚合到一起
      Map(51 ->  51:55:2, 51:52:2, 51:53:2, 51:56:1
        56 ->  56:53:1, 56:55:1, 56:52:1,56:51:1
        )
     */
    def itemMatrixGroupByItemId(itemAndItemMatrixCollection: Map[String, Int]):  Map[String, Iterable[Array[String]]] = {
println("----- 根据 ItemId , groupBy ItemMatrix  -----")

        val itemAndItemGroup = itemAndItemMatrixCollection.map( f => {
            val ids = f._1.split(":")
            val itemId = ids(0).toString()        // 房源 ID
            val itemRsId = ids(1).toString()      // 推荐房源 ID
            val itemRsIdCnt = f._2.toString()     // 共同看过的人数
            // 转换成数组
            Array(itemId, itemRsId, itemRsIdCnt)
            //println(itemId, itemRsId, itemRsIdCnt)
        }).groupBy { 
            // 然后, 按照 itmeId 把把同类的 Item ID groupBy 到一起
            f => f(0)
        }

        //itemAndItemGroup
        scala.collection.mutable.Map(itemAndItemGroup.toSeq:_*)
    }
    
}