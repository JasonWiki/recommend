package com.angejia.dw.recommend.user

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.angejia.dw.recommend.Conf
import com.angejia.dw.hadoop.hbase.HBaseClient

/**
 * UBCF 算法实现
 *
 * create 'userUBCF',{NAME=>'relation'},{NAME=>'recommend'}
 * relation 用户关系
 * recommend 用户推荐
 */
object UserUBCF20160517 {

    // hbase 数据表
    var userUBCFHbaseTable: HBaseClient = null

    // 等待训练的数据文件
    var characteristicsFile: String = null

    // 文件分隔符
    var separator = "\t"

    def main(args: Array[String]) {

        for (ar <- args) {
            println(ar)
        }

        val env = args(0)
        this.init(env)

        this.characteristicsFile = args(1)

        //this.separator = " "
        //this.characteristicsFile = "/data/log/recommend/recommend_user_inventory_history/mytest"

        // 执行算法
        this.calculate()
    }

    /**
     * 初始化
     * env:  dev 开发环境， online 线上环境
     */
    def init(env: String): Unit = {
        Conf.setEnv(env)

        // 连接 userUBCF 数据表
        this.userUBCFHbaseTable = new HBaseClient("userUBCF", Conf.getZookeeperQuorum())
    }

    /**
     * 算法逻辑
     */
    def calculate(): Unit = {
        val conf = new SparkConf()
        conf.setAppName("UserUBCF")
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
            val lineArr = line.split(this.separator)
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

        println("----- 生成 user -> items 集合表 -----")
        val userItemsCollectionRDD = filterSourceDataRDD.map { line =>
            val lineArr = line.split(this.separator)
            val userId = lineArr.apply(0).toInt
            val inventoryId = lineArr.apply(1).toInt
            val cnt = lineArr.apply(2).toInt // 感兴趣的次数
            (userId, (inventoryId, cnt))
        }.groupByKey()
        userItemsCollectionRDD.take(10).foreach(println)
        println("")

        println("----- 生成 item -> user 集合倒序表 -----")
        val itemsUserCollectionRDD = filterSourceDataRDD.map { line =>
            val lineArr = line.split(this.separator)
            val userId = lineArr.apply(0).toInt
            val inventoryId = lineArr.apply(1).toInt
            (inventoryId, userId)
        }.groupByKey()
        itemsUserCollectionRDD.take(10).foreach(println)
        println("")

        println("-----  基于 item 下的 users 集合 , 为每个 item 生个 user -> uesr 相似度矩阵 B -----")
        // 每个用户相似度矩阵
        val userMatrixsRDD = itemsUserCollectionRDD.map { itemAndUsers =>
            val itemId = itemAndUsers._1
            val itemUsers = itemAndUsers._2

            /**
             * 基于 item 下的 user 集合, 为每个 item 下的 users 集合生成一个矩阵 B
             * 数据结构
             * Map(10:30 -> 1, 30:10 -> 1)
             */
            val userMatrix: Map[String, Int] = Map[String, Int]()

            // 遍历当前 item 下的 users 集合
            for (u <- itemUsers) {
                for (v <- itemUsers) {
                    // 排除相同的 user
                    if (u != v) {
                        // 默认为每个用户 +1 个访问次数
                        //userMatrix += Map(u -> Map(v -> 1))
                        val key = u.toString() + ":" + v.toString()
                        userMatrix.put(key, 1)
                    }
                }
            }

            userMatrix
        }
        userMatrixsRDD.take(10).foreach(println)
        println("")

        println("-----  合并所有 user -> uesr 相似度矩阵 B , 生成 C 矩阵(包含所有用户相似度的矩阵) ----- ")

        val userRelationMatrix = userMatrixsRDD.reduce { (x, y) =>
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
                    curMatrix.put(yK, yV)
                }
            }
            curMatrix
        }
        userRelationMatrix.take(10).foreach(println)
        println("")

        println("----- C 矩阵聚合同类型 user 到 -> userRelationMatrixCollection 集合  ----- ")
        val userRelationMatrixCollection = userRelationMatrix.map(f => {
            val ids = f._1.split(":")
            val userId = ids(0).toString() // user ID
            val userRelationId = ids(1).toString() // 相似 user ID
            val userRelationIdCnt = f._2.toString() // 相似次数
            // 转换成数组
            Array(userId, userRelationId, userRelationIdCnt)
            // 把
        }).groupBy {
            // 然后, 按照 userId 把把同类的userId groupBy 到一起
            f => f(0)
        }
        userRelationMatrixCollection.take(10).foreach { userRelation => println(userRelation._1, " -> 关联用户集合: ", userRelation._2.map { x => x.mkString(":") }.mkString(", ")) }
        println("")

        println("----- Start : 基于 userRelationMatrixCollection 集合, 持久化数据到 Hbase ----- \n")

        /**
         * users -> items 集合本地化
         * return
         *    Map(
         *       userId1 -> Iterable[(ItemId, 感兴趣次数)),
         *       userId2 -> Iterable[(ItemId, 感兴趣次数))
         *    )
         */

        println("----- userId -> items 集合本地化 ")
        val userItemsCollectionMap: scala.collection.immutable.Map[Int, Iterable[(Int, Int)]] = userItemsCollectionRDD.collect().toMap
        userItemsCollectionMap.take(10).foreach(println)
        println("")

        println("----- user 关联结果, user 推荐结果, 持久化到 Hbase Table ")
        var userRelationMatrixLineNum = 0 // user 关系矩阵行数
        var userRecommendLineNum = 0 // user 推荐行数

        // 遍历 user -> users 相似度集合
        userRelationMatrixCollection.foreach { userRelationInfo =>

            val curUserId = userRelationInfo._1 // userId
            val curUserRelationUsers = userRelationInfo._2 // 关联的 users 集合 

            // ---------- Start user -> user 集合关系保存到 Hbase ----------

            val userRelationToString = curUserRelationUsers.map(userRelation => userRelation.mkString(":")).mkString(",")
            this.userRelationMatrixWriteHbase(curUserId, userRelationToString)

            // ---------- End user -> user 集合关系保存到 Hbase ----------

            // ---------- Start 为 user 推荐 items ----------

            // 保存当前 user 最终推荐的 items
            val userRecommendItemsRs = ListBuffer[Array[String]]()
            //val userRecommendItems = ListBuffer[Map[String,String]]()

            // 当前 user 已经推荐的 item Ids , 用来保存已经存在 itemIds
            //val userRecommendItemsPool = ListBuffer[String]()
            val userRecommendItemsPool = Map[String, String]()

            // 当前 user 自身的 items 
            val curUserItems = userItemsCollectionMap.getOrElse(curUserId.toInt, null)
            if (curUserItems != null) {
                curUserItems.foreach { itemInfo =>
                    val itemId = itemInfo._1
                    val itemPf = itemInfo._2

                    // 组合推荐结果
                    val rsInfo = Array(
                        curUserId, // 当期 userId
                        "0", // 关联 userId (因为是自身的所以用 0 表示)
                        "0", // 关联 user 相似度分数  (因为是自身的所以用 0 表示)
                        itemId.toString(), // 关联 user ItemId
                        itemPf.toString() // 关联 user item 喜欢次数
                        )

                    userRecommendItemsRs.append(rsInfo)
                }
            }

            // 当前 user Relation user 下的 items 集合, 把当前 user items 不存在的 item 追加进去, 最终汇总后作为推荐结果
            curUserRelationUsers.foreach { relationUserInfo =>
                val relationUserId = relationUserInfo.apply(1) // 关联 uesrId
                val relationUserPf = relationUserInfo.apply(2) // 关联 user 的相似度分数

                // 相似度分数大于 1 才会推荐
                if (relationUserPf.toInt > 1) {
                    // 关联 user 的 items
                    val relationUserItems = userItemsCollectionMap.getOrElse(relationUserId.toInt, null)
                    if (relationUserItems != null) {

                        relationUserItems.foreach { itemInfo =>
                            val itemId = itemInfo._1
                            val itemPf = itemInfo._2

                            // 若推荐的 items 已经存在, 则不推荐了
                            if (!userRecommendItemsPool.contains(itemId.toString())) {
                                // 组合推荐结果
                                val rsInfo = Array(
                                    curUserId, // 当期 userId
                                    relationUserId, // 关联 userId
                                    relationUserPf, // 关联 user 相似度分数
                                    itemId.toString(), // 关联 user ItemId
                                    itemPf.toString() // 关联 user item 喜欢次数
                                    )

                                // userRecommendItemsPool.append(itemId.toString())
                                userRecommendItemsPool.put(itemId.toString(), "exist")

                                userRecommendItemsRs.append(rsInfo)
                            }
                            // println(rsInfo.toBuffer)
                            /**
                             * userRecommendItems.append(Map(
                             * "userId" -> curUserId,     // 当期 userId
                             * "relationUserId" -> relationUserId,     // 关联 userId
                             * "relationUserPf" -> relationUserPf,     // 关联 user 相似度分数
                             * "relationUserItemId" -> itemId.toString(),    // 关联 user ItemId
                             * "relationUserItemPf" -> itemPf.toString()    // 关联 user item 喜欢次数
                             * ))
                             */
                        }
                    }
                }
            }

            // 当前推荐结果转换成 字符串
            val userRecommendItemsToString = userRecommendItemsRs.map(recommendItemInfo => recommendItemInfo.mkString(":")).mkString(",")
            // println(curUserId, userRecommendItemsRs.size)

            // userRecommendItemsRs.foreach{ f => println(f.toBuffer) }
            this.userRecommendWriteHbase(curUserId, userRecommendItemsToString)

            // ---------- End 为 user 推荐 items ----------
            if (!userRecommendItemsRs.isEmpty) userRecommendLineNum += 1
            userRelationMatrixLineNum += 1
        }
        println("")
        println("-----  HBase Table userUBCF: ",
            " userRelationMatrixLineNum 写入了: " + userRelationMatrixLineNum + " 行",
            " userRecommendLineNum 写入了: " + userRecommendLineNum + " 行")
        println("")

        println("----- End : 基于 userRelationMatrixCollection 集合, 持久化数据到 Hbase -----")

        /**
         * println("----- userRelationMatrixCollection 集合持久化到 Hbase -----")
         * userRelationMatrixCollection.foreach{ line =>
         * val userId = line._1
         * val userRelationInfo = line._2
         * // 把里面的 array 按照:组合, 最外层按照,组合
         * val userRelationToString = userRelationInfo.map(userRelation => userRelation.mkString(":")).mkString(",")
         *
         * blankLines += 1
         * this.userRelationMatrixWriteHbase(userId, userRelationToString)
         * }
         * println("HBase Table : userRelation 写入了: " + blankLines + " 行")
         */

    }

    /**
     * uesr 关系矩阵, 写入到 Hbase
     */
    def userRelationMatrixWriteHbase(rowKey: String, value: String): Unit = {
        this.userUBCFHbaseTable.insert(rowKey, "relation", "userRelation", value)
    }

    /**
     * 保存推荐结果到 Hbase
     */
    def userRecommendWriteHbase(rowKey: String, value: String): Unit = {
        this.userUBCFHbaseTable.insert(rowKey, "recommend", "userRecommend", value)
    }

}