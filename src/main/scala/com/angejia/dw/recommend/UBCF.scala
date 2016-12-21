package com.angejia.dw.recommend

import scala.collection.mutable.Map

import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class UBCF {
    
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

    var filterModel: RDD[String] = null

    /**
     * 计算矩阵并得到 user -> uesr 关系矩阵 - 根据文件
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

        // 生成 user -> user 矩阵
        val userAndUserMatrixRDD = this.generateUserAndUserMatrix(modelRDD, this.getFileSeparator())

        // 合并所有 user -> user 矩阵
        val userAndUserMatrixCollection = this.mergeUserAndUserMatrix(userAndUserMatrixRDD)

        userAndUserMatrixCollection
    }



     /**
     * 计算矩阵并得到 user -> uesr 关系矩阵 - 根据基础输入模型
     */
    def calculateByBaseModelRDD(baseModelRDD: RDD[String], fileSeparator: String): Map[String, Int] = {

        this.setFileSeparator(fileSeparator)

        // 过滤模型
        val modelRDD =  this.filterModelRDD(baseModelRDD, this.getFileSeparator())

        // 生成 user -> user 矩阵
        val userAndUserMatrixRDD = this.generateUserAndUserMatrix(modelRDD, this.getFileSeparator())

        // 合并所有 user -> user 矩阵
        val userAndUserMatrixCollection = this.mergeUserAndUserMatrix(userAndUserMatrixRDD)

        userAndUserMatrixCollection
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
//                if (userId.matches("[0-9]+") == false) {
//                    checkStatus = false
//                }
//                if (itemId.matches("[0-9]+") == false) {
//                    checkStatus = false
//                } 
                if (userId == "" || itemId == "") {
                    checkStatus = false
                }
            }
            checkStatus
        }
        
        this.filterModel = filterRDD
        filterRDD
    }



    /**
     * 基于 item 下的 users 集合 , 为每个 item 生个 user -> uesr 相似度矩阵 B
     * modelRDD : 基础数据模型
     * return RDD(
     *     Map("50:57"->1, "57:50"->1, "51:55"->1),
     *     Map("50:57"->2, "57:50"->3, "51:55"->4)
     * )
     */
    def generateUserAndUserMatrix(modelRDD:  RDD[String], fileSeparator: String) :  RDD[Map[String, Int]] = {

println("----- 生成 item -> user 集合倒序表 -----")
        val itemsUserCollectionRDD = modelRDD.map { line =>
            val lineArr = line.split(fileSeparator)
            val userId = lineArr.apply(0).toString()
            val itemId = lineArr.apply(1).toString()
            (itemId, userId)
        }.groupByKey()


println("----- 基于 item 下的 users 集合 , 为每个 item 生个 user -> uesr 相似度矩阵 B -----")
        // 每个用户相似度矩阵
        val userMatrixsRDD = itemsUserCollectionRDD.map{ itemAndUsers =>
            val itemId = itemAndUsers._1
            val itemUsers = itemAndUsers._2

            /**
             * 基于 item 下的 user 集合, 为每个 item 下的 users 集合生成一个矩阵 B
             * 数据结构
                 Map(10:30 -> 1, 30:10 -> 1)
             */
            val userMatrix : Map[String,Int] = Map[String,Int]()

            // 遍历当前 item 下的 users 集合
            for (u <- itemUsers) {
                for (v <- itemUsers) { 
                    // 排除相同的 user
                    if (u != v) {
                        // 默认为每个用户 +1 个访问次数
                        //userMatrix += Map(u -> Map(v -> 1))
                        val key = u.toString() + ":" + v.toString()
                        userMatrix.put(key ,1)
                    }
                }
            }

            userMatrix
        }
        userMatrixsRDD
    }



    /**
     *  合并所有 user -> uesr 相似度矩阵 B , 生成 C 矩阵(包含所有用户相似度的矩阵) 矩阵
     *  userAndUserMatrix : item -> item 矩阵
     */
    def mergeUserAndUserMatrix(userAndUserMatrix: RDD[Map[String, Int]]): Map[String, Int] = {
println("----- 合并所有 user -> uesr 相似度矩阵 B , 生成 C 矩阵(包含所有用户相似度的矩阵) ----- ")

        var rsUserMatrix: Map[String, Int] = Map[String, Int]()

        if (userAndUserMatrix.count() == 0) {
           return rsUserMatrix
        }

        // 合并最终的矩阵
        val userAndUserMatrixCollection = userAndUserMatrix.reduce{ (x, y) =>

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

        userAndUserMatrixCollection
    }



    /**
     * 根据 userId , groupBy userRelationMatrix
      把同类型的物品, 聚合到一起
      Map(51 ->  51:55:2, 51:52:2, 51:53:2, 51:56:1
        56 ->  56:53:1, 56:55:1, 56:52:1,56:51:1
        )
     */
    def userRelationMatrixGroupByUserId(userRelationMatrix: Map[String, Int]):  Map[String, Iterable[Array[String]]] = {
//println("----- C 矩阵聚合同类型 user 到 -> userRelationMatrixCollection 集合  ----- ")
println("----- 根据 userId , groupBy userRelationMatrix  ----- ")

        val userRelationGroup = userRelationMatrix.map( f => {
            val ids = f._1.split(":")
            val userId = ids(0).toString()               // user ID
            val userRelationId = ids(1).toString()      // 相似 user ID
            val userRelationIdCnt = f._2.toString()     // 相似次数
            // 转换成数组
            Array(userId, userRelationId, userRelationIdCnt)
        }).groupBy { 
            // 然后, 按照 userId 把把同类的 userId groupBy 到一起
            f => f(0)
        }

        scala.collection.mutable.Map(userRelationGroup.toSeq:_*)

    }



    /**
     * 生成 user -> items 集合
     */
    def userAndItemsCollection() :  RDD[(String, Iterable[(String, Int)])] = {
        val fileSeparator = this.getFileSeparator()

println("----- 生成 user -> items 集合表 -----")
        val userItemsCollectionRDD = this.filterModel.map { line =>
            val lineArr = line.split(fileSeparator)
            val userId = lineArr.apply(0).toString()    
            val itemId = lineArr.apply(1).toString()
            val cnt = lineArr.apply(2).toInt    // 感兴趣的次数
            (userId, (itemId, cnt))
        }.groupByKey()
        
        userItemsCollectionRDD
    }
}