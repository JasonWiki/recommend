package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


import com.angejia.dw.common.util.DateUtil
import com.angejia.dw.common.util.JsonUtil
import com.angejia.dw.common.util.mysql.MysqlClient
import com.angejia.dw.common.util.parse.ParseMobileAgent
import com.angejia.dw.common.util.parse.ParseMobileToken
import com.angejia.dw.hadoop.hbase.HBaseClient
import com.angejia.dw.hadoop.hive.HiveClient
import com.angejia.dw.hadoop.kafka.KafkaProducer
import com.angejia.dw.recommend.Conf
import com.angejia.dw.recommend.inventory.portrait.InventoryPortraitCommon

/**
 * 用户画像建模入口
 *
 * Hbash 用户画像表结构
 * create 'userPortrait',{NAME=>'tags'},{NAME=>'dimension'},{NAME=>'needs'},{NAME=>'modelState'}
 */

object UserPortrait {

    case class Config(env: String = "dev", kafkaTopic: String = "", batchDuration: Int = 5,
        kafkaPartitionNum: Int = 1, kafkaConsumerGroupId: String = "")

    var config: Option[Config] = null

    // kafkaProducer 连接对象
    var kafkaProducer: KafkaProducer = null

    def main(args: Array[String]) {
        val parser = new scopt.OptionParser[Config]("UserPortrait") {
            head("UserPortrait")

            opt[String]('e', "env").required().valueName("<env>").
                action((x, c) => c.copy(env = x)).
                text("环境名称 dev为线下，online为线上")

            opt[Int]('b', "batch-duration").valueName("<duration>").
                action((x, c) => c.copy(batchDuration = x)).
                text("spark steaming 的 batchDuration 默认为5")

            opt[String]('t', "kafka-topic").required().valueName("<topic>").
                action((x, c) => c.copy(kafkaTopic = x)).
                text("Kafka消费的topic 比如accessLog")

            opt[Int]('p', "kafka-partition-num").valueName("<num>").
                action((x, c) => c.copy(kafkaPartitionNum = x)).
                text("Kafka消费topic的PartitionNum 默认为1")

            opt[String]('g', "kafka-consumer-gid").required().valueName("<gid>").
                action((x, c) => c.copy(kafkaConsumerGroupId = x)).
                text("Kafka消费者的gid")

            help("help").text("输出此帮助信息")

            note("启动spark steaming来消费accesslog")
        }

        this.config = parser.parse(args, Config())

        if (!this.config.isEmpty) {
            this.init(this.config.get.env)
            this.runByDirectApproach()
            //this.test()
        }
    }

    def test(): Unit = {

        // 697 281 347014 459671 41560 
        val userId = 613.toString()
        val cityId = "1";

        //UserPortraitMemberDemand.setUserId(userId)
        //UserPortraitMemberDemand.setCityId(cityId)
        //UserPortraitMemberDemand.run()

        //val str1 = "/mobile/member/inventories/list?city_id=1&community_id=18281&bedroom_id=2&price_id=6&district_id=7&block_id=62"
        //UserPortraitFilter.setUserId(userId)
        //UserPortraitFilter.setRequestUri(str1)
        //UserPortraitFilter.run()

        // 1055 1003
        var inventoryPageUri = "/mobile/member/inventories/1003/167803"

        //UserPortraitBrowse.setUserId(userId)
        //UserPortraitBrowse.setRequestUri(inventoryPageUri)
        //UserPortraitBrowse.run()

        //inventoryPageUri = "/mobile/member/inventory/detail/452447/0?by=451323"
        //inventoryPageUri = "/mobile/member/inventory/detail/452447/0"
        //UserPortraitBrowse.setUserId(userId)
        //UserPortraitBrowse.setRequestUri(inventoryPageUri)
        //UserPortraitBrowse.run()

        // 喜欢房源
        //UserPortraitLikeInventory.setUserId(userId)
        //UserPortraitLikeInventory.run()

        // 昨天日期
        val yesterDate = DateUtil.getCalendarOffsetDateDay(-1)
        val yesterYmd = DateUtil.DateToString(yesterDate, DateUtil.SIMPLE_Y_M_D_FORMAT)

        // 发生过带看
        UserPortraitVisitItem.run(userId, yesterYmd)

        // 发生过连接
        //UserPortraitLinkInventory.run(userId, yesterYmd)

        //exit()
    }

    def init(env: String): Unit = {
        Conf.setEnv(env)

        // 初始化 produc Mysql 配置
        val productMysqDBInfo = Conf.getProductMysqDBInfo()
        UserPortraitCommon.mysqlClient = new MysqlClient(productMysqDBInfo.getOrElse("host", ""), productMysqDBInfo.getOrElse("account", ""), productMysqDBInfo.getOrElse("password", ""), productMysqDBInfo.getOrElse("defaultDB", ""))

        InventoryPortraitCommon.mysqlClient = UserPortraitCommon.mysqlClient

        // 初始化 Hbase 用户画像表
        UserPortraitCommon.userPortraitTable = new HBaseClient("userPortrait", Conf.getZookeeperQuorum())

        // spark 客户端配置
        val sparkConf = Conf.getSparkConf()
        var thriftServerUrl = sparkConf.getOrElse("sparkThriftServerUrl", "")
        var thriftServerUser = sparkConf.getOrElse("sparkThriftServerUser", "")
        var thriftServerPass = sparkConf.getOrElse("sparkThriftServerPass", "")
        // 初始化 spark 操作 hive 客户端
        UserPortraitCommon.sparkHiveClient = new HiveClient(thriftServerUrl, thriftServerUser, thriftServerPass)

    }

    

    /**
     * 直流模式
     */
    def runByDirectApproach() : Unit = {

        // 创建 sparkStreaming 上下文对象
        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("UserPortrait")
        conf.set("spark.streaming.kafka.maxRetries"  , "5")

        val ssc = new StreamingContext(conf, Seconds(this.config.get.batchDuration))

        val brokerList = Conf.getKafkaServerBrokerList()
        val topics = Array(this.config.get.kafkaTopic)
        val groupId = this.config.get.kafkaConsumerGroupId

        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> brokerList,
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> groupId,
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        //println(kafkaParams)

        val directKafkaStream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )


        directKafkaStream.foreachRDD{ (rdd, time) =>

            println("------ " + "RDD : " + rdd + " , Time: " + time + " --------- ")

            if (rdd.isEmpty() == false && rdd.partitions.length != 0 ) {

                println("------ " + "PartitionsNum : " + rdd.getNumPartitions + " --------- ")

                rdd.foreachPartition{ iteratorConsumerRecord => 

                    // 初始化对象
                    var iteratorConsumerRecordMap = iteratorConsumerRecord.map{ consumerRecord =>

                        // 当前批次日志
                        var consumerRecordVal = consumerRecord.value().split("\n")

                        if (consumerRecordVal.size > 0) {

                            // 处理每条详细的日志
                            consumerRecordVal.map{ curLine =>
                                var logInfo = this.formatLogData(curLine)
                                //println(logInfo)

                                val logType = logInfo.getOrElse("logType", "").toString()
                                logType match {
                                    case "accessLog" => {
 
                                        this.recommendAction(logInfo)
                                        //println("print")
                                    }
                                    case _ => println("nothing")
                                }

                                logInfo = null
                            }
                        }

                        consumerRecordVal = null
                    }

                    // 批处理条数
                    println("------ " + "Consumer Record 批处理量: " + iteratorConsumerRecordMap.length + " ------ ")
                    iteratorConsumerRecordMap = null
                }

            }
        }

        // 启动流计算环境 StreamingContext 并等待它"完成"
        ssc.start()
        // 等待作业完成
        ssc.awaitTermination()

    }



    /**
     * 推荐控制
     */
    def recommendAction(logInfo: HashMap[String, String]): Unit = {

        if (logInfo.contains("userId")) {

            val userId = logInfo.getOrElse("userId", "").toString()
            val cityId = logInfo.getOrElse("cityId", "").toString()
            val uri = logInfo.getOrElse("logRequestUri", "").toString()

            // 数据验证
            if (userId.isEmpty() || userId == "" || uri.isEmpty() || cityId.isEmpty()) return

            println("------------------------------------------------ O ")
            //println(userId, " -> ", cityId)
            println(logInfo)

            // 筛选和搜索小区打分
            UserPortraitFilter.setUserId(userId)
            UserPortraitFilter.setRequestUri(uri)
            val filterReStatus = UserPortraitFilter.run()

            // 浏览房源单页
            UserPortraitBrowse.setUserId(userId)
            UserPortraitBrowse.setRequestUri(uri)
            val browseReStatus = UserPortraitBrowse.run()

            // 收藏房源单页, 可以通过监听收藏 api 来触发优化
            UserPortraitLikeInventory.setUserId(userId)
            val likeReStatus = UserPortraitLikeInventory.run()

            // 用户选房单打分逻辑, 可以通过监听选房单 api 来触发优化
            UserPortraitMemberDemand.setUserId(userId)
            UserPortraitMemberDemand.setCityId(cityId)
            val demandReStatus = UserPortraitMemberDemand.run()

            // 获取昨天的日期
            val yesterDate = DateUtil.getCalendarOffsetDateDay(-1)
            val yesterYmd = DateUtil.DateToString(yesterDate, DateUtil.SIMPLE_Y_M_D_FORMAT)

            // 发生过带看的房源打分逻辑
            val visitItem = UserPortraitVisitItem.run(userId, yesterYmd)

            // 发生过连接的房源打分逻辑
            val linkInventory = UserPortraitLinkInventory.run(userId, yesterYmd)

            // 当用户有操作行为, 则发送推荐
            if (filterReStatus == "yes"
                || browseReStatus == "yes"
                || likeReStatus == "yes"
                || demandReStatus == "yes"
                || visitItem == "yes"
                || linkInventory == "yes"
             ) {
                
                //val userMap = Map("user_id" -> userId, "city_id" -> cityId)
                //val userJson = JsonUtil.playMapToJson(userMap)
                println("nothing ~O(∩_∩)O~")
            }
            println("------------------------------------------------ X ")
        }

    }

    /**
     * 格式化日志成 Map 数据结构
     */
    def formatLogData(line: String): HashMap[String, String] = {
        val curLineArr = line.split("\t")
        // println(curLineArr.toBuffer)

        val result: HashMap[String, String] = new HashMap[String, String]()

        if (curLineArr.length > 15) {

            val logTime = curLineArr(5) // 日志请求时间
            val logHost = curLineArr(6) // 日志请求 host

            //解析出 url
            val logRequestUris = curLineArr(7).split(" ")
            var logRequestUri = ""
            if (logRequestUris.length >= 2)
                logRequestUri = logRequestUris(1) 

            val auth = curLineArr(14) // 加密后的用户信息

            val appAgent = curLineArr(15) // appAgent

            result.put("logType", "accessLog")
            result.put("logTime", logTime)
            result.put("logHost", logHost)
            result.put("logRequestUri", logRequestUri)
            result.put("auth", auth)
            result.put("appAgent", appAgent)

            if (!auth.isEmpty()) {
                //result.put("userId", this.getUserId(auth)._1) 
                //result.put("userJson", this.getUserId(auth)._2)
                result.put("userId", this.getUserId(auth))
            }
            if (!appAgent.isEmpty()) {
                result.put("cityId", this.getCityId(appAgent));
            }

        }

        result
    }

    def getUserId(auth: String): String = {
        val userId = ParseMobileToken.evaluate(auth, "user_id")
        userId
    }

    def getCityId(appAgent: String): String = {
        val cityId = ParseMobileAgent.evaluate(appAgent, "ccid")
        cityId
    }

}

