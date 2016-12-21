package com.angejia.dw.recommend.user

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.angejia.dw.recommend.Conf
import com.angejia.dw.hadoop.hbase.HBaseClient
import com.angejia.dw.recommend.UBCF

/**
 * UBCF 算法实现
 *
 * create 'userUBCF',{NAME=>'relation'},{NAME=>'recommend'},{NAME=>'baseInfo'}
 * relation 用户关系
 * recommend 用户推荐
 */
object UserUBCF {

    // hbase 数据表
    var hbaseResultTb: HBaseClient = null

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

        this.calculate()
    }

    /**
     * 初始化
     * env:  dev 开发环境， online 线上环境
     */
    def init(env: String): Unit = {
        Conf.setEnv(env)

        // 连接 userUBCF 数据表
        this.hbaseResultTb = new HBaseClient("userUBCF", Conf.getZookeeperQuorum())
    }

    def calculate(): Unit = {
        /**
         * 初始化 spark
         */
        val sparkConf = new SparkConf()
        sparkConf.setAppName("UserUBCF")
        sparkConf.setMaster("local[2]")

        /**
         * 初始化推荐模型
         */
        val userUBCF = new UBCF()

        // user -> uesr 相似度矩阵
        val userAndUserMatrixCollection = userUBCF.calculateByFile(sparkConf, characteristicsFile, separator)

        // 根据  userId , groupBy userRelationMatrix
        val userRelationGroup = userUBCF.userRelationMatrixGroupByUserId(userAndUserMatrixCollection)

        // user -> items 集合
        val userItemsCollectionRDD = userUBCF.userAndItemsCollection()

        println("----- userId -> items 集合本地化 ")
        val userItemsCollectionMap: scala.collection.immutable.Map[String, Iterable[(String, Int)]] = userItemsCollectionRDD.collect().toMap

        println("----- Start : 基于 userRelationGroup 集合, 持久化数据到 Hbase ----- \n")

        println("----- user 关联结果, user 推荐结果, 持久化到 Hbase Table ")
        var userRelationMatrixLineNum = 0 // user 关系矩阵行数
        var userRecommendLineNum = 0 // user 推荐行数

        // 遍历 user -> users 相似度集合
        userRelationGroup.foreach { userRelationInfo =>

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
            val curUserItems = userItemsCollectionMap.getOrElse(curUserId, null)
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
                    val relationUserItems = userItemsCollectionMap.getOrElse(relationUserId, null)
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

        println("----- End : 基于 userRelationGroup 集合, 持久化数据到 Hbase -----")

    }

    /**
     * uesr 关系矩阵, 写入到 Hbase
     */
    def userRelationMatrixWriteHbase(rowKey: String, value: String): Unit = {
        this.hbaseResultTb.insert(rowKey, "relation", "userRelation", value)
    }

    /**
     * 保存推荐结果到 Hbase
     */
    def userRecommendWriteHbase(rowKey: String, value: String): Unit = {
        this.hbaseResultTb.insert(rowKey, "recommend", "userRecommend", value)
    }

}