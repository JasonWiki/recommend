package com.angejia.dw.recommend.inventory

import play.api.libs.json._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.angejia.dw.hadoop.spark.CollaborativeFiltering
import com.angejia.dw.common.util.{FileUtil}
import com.angejia.dw.hadoop.hbase.HBaseClient


/**
 * 这个是 spark mlib 算法实现的,不靠谱呵呵哒
 *
 * create 'inventoryRecommend',{NAME=>'inventoryRecommendInventory'}


  spark-submit \
  --name InventoryIBCF \
  --class com.angejia.dw.recommend.inventory.InventoryIBCF \
  --master local[2] \
   ~/app/recommend/recommend-2.0/target/scala-2.10/recommend-2.0.jar "DataNode01" "inventoryRecommend" "/data/log/recommend/ml-100k/u.data"

   参数: 
   [zookeeperIds] [HBaseTableName] [characteristicsFile]
 */
object InventoryIBCFspark  {

    var zookeeperIds: String = null
    var HBaseTableName: String = null
    var HBaseClientService: HBaseClient = null
    var characteristicsFile: String = null

    def main(args: Array[String])  {

        for (ar <- args) {
            println(ar)
        }

        // Hbase 配置
        zookeeperIds = args(0)
        HBaseTableName = args(1)
        HBaseClientService = new HBaseClient(HBaseTableName,zookeeperIds)

        // 等待训练的文件
        characteristicsFile = args(2)
        //characteristicsFile = "/data/log/recommend/recommend_user_inventory_history/mytest"

        val inventoryIBCF = new InventoryIBCFspark()
        inventoryIBCF.characteristicsFile = characteristicsFile
        inventoryIBCF.run()
    }
 

    /**
     * 写 HBase
     */
    def resultWriteHBase(rowKey: String, value: String) : Unit = {
        HBaseClientService.insert(rowKey, "inventoryRecommendInventory", "inventoryIds", value)
    }

}

/**
 * 看了又看算法
 */
class InventoryIBCFspark extends Serializable {

    // 提取特征的文件 
    var characteristicsFile: String = null 

    def run(): Unit = {
        Logger.getRootLogger.setLevel(Level.WARN)

        // 训练模型
        val inventoryTrainModel = this.inventoryTrain()
    }


    /**
     * 训练 inventory 模型
     */
    //def inventoryTrain() : MatrixFactorizationModel = {
    def inventoryTrain() : Unit = {

println("----- 开始初始化 -----")
        // SPARK 运行环境配置
        val conf = new SparkConf()
        conf.setAppName("InventoryIBCF")
        conf.setMaster("local[2]")

        conf.set("spark.cores.max", "4") // 16 map workers, that is 2 workers per machine (see my cluster config below) 
        //conf.set("spark.akka.frameSize", "100000") 
        conf.set("spark.driver.maxResultSize", "2g") 
        conf.set("spark.executor.memory", "2g") 
        conf.set("spark.reducer.maxMbInFlight", "100000") 
        conf.set("spark.storage.memoryFraction", "0.9") 
        conf.set("spark.shuffle.file.buffer.kb", "1000") 
        conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")   

        val sc = new SparkContext(conf)

        // 读取数据源
        val sourceDataRDD = sc.textFile(characteristicsFile)
        //println(sourceDataRDD.first())

        // 把行分割成数组，并且读取数组前 3 个元素 ，格式化后作为入参
        val ratingsRDD = sourceDataRDD.map(line => {
            val curLine = line.split("\t").map { x => x.toInt}
            Array(curLine(0), curLine(1) , curLine(2))
        })
        ratingsRDD.take(2).foreach(x => println(x(0) + ":" + x(1) + ":" + x(2)))


        // 把需要推荐的房源 id 抽取出来,去重
        val needRecommendInventoryIdsRDD = ratingsRDD.map(_(1)).distinct()
        needRecommendInventoryIdsRDD.take(2).foreach(x => println(x))
         

        // 算出所有房源相关度 (调试)
        //val inventoryIds = List(308524, 213775, 276360, 206754)
        //val needRecommendInventoryIdsRDD = sc.parallelize(inventoryIds)
        //needRecommendInventoryIdsRDD.take(4).foreach {x => println(x)}

        // 计算需要推荐的房源的次数
        val inventoryIdsRddCount = needRecommendInventoryIdsRDD.count().toInt
println("共需要推荐: " + inventoryIdsRddCount)


println("----- 提取特征 -----")
        // IBCF 算法类
        val collaborativeFiltering = new CollaborativeFiltering()

        // 提取特征
        val characteristicsRDD = collaborativeFiltering.characteristics(ratingsRDD)


println("----- 训练模型 -----")
        // 训练模型
        collaborativeFiltering.train(characteristicsRDD, 50, 10, 0.01)
        // 广播变量
        val collaborativeFilteringSignPrefixesRdd = sc.broadcast(collaborativeFiltering)

        // 累加器
        val blankLines = sc.accumulator(0)


println("----- inventoryId 计算推荐的 inventoryIds -----")
        val inventoryResInventorysRDD = needRecommendInventoryIdsRDD.map { inventoryId => 
          val itemCosineSimilarity = collaborativeFilteringSignPrefixesRdd.value.itemCosineSimilarity(inventoryId)

          var result = ""
           itemCosineSimilarity.take(100).foreach{ inventroyRes =>
               val inventoryRId = inventroyRes._1    // 推荐的房源 ID
               val inventoryRSouce = inventroyRes._2    // 推荐的房源 分数
               result += inventoryId + ":" + inventoryRId + ":" + inventoryRSouce + ","
           }
           blankLines += 1
           println(blankLines)
           println("wirete: " + result)
          // 结果写到 HBase
          //InventoryIBCF.resultWriteHBase(inventoryId.toString(),result.toString())
        }.take(inventoryIdsRddCount)



/**
println("----- inventoryId 计算推荐的 inventoryIds -----")
        // 为每个 inventoryId 计算推荐的 inventoryIds
        val inventoryResInventorysRDD = needRecommendInventoryIdsRDD.map { inventoryId => 

            /** 物品余弦相似度计算
             *  RDD[(Int, Double)] 返回详细值: 
                 (1,0.537378279119025)
                 (3,0.37167637258108627)
                 (5,0.6282701874791976)
             */
            val itemCosineSimilarity = collaborativeFilteringSignPrefixesRdd.value.itemCosineSimilarity(inventoryId)

            val id = inventoryId
            val result: Array[(Int, Double)] = itemCosineSimilarity.take(5000)

            val t: (Int,Array[(Int, Double)]) = (id, result)
            t
        }

        // 计算需要推荐的房源的次数
        val inventoryIdsRddCount = needRecommendInventoryIdsRDD.count().toInt


println("----- 准备开始写入数据 -----")
        /** 格式化数据
         *  写入到 Hbase 中             
        */
        val data = inventoryResInventorysRDD.map(inventoryData => {
           val inventoryId = inventoryData._1    // 123234  这种数据结构s
           val ResInventorys = inventoryData._2    // (2,0.5074470833019032) 这种数据结构

           var result = ""
           ResInventorys.foreach{ tuple =>
               val inventoryRId = tuple._1    // 推荐的房源 ID
               val inventoryRSouce = tuple._2    // 推荐的房源 分数
               result += inventoryId + ":" + inventoryRId + ":" + inventoryRSouce + ","
           }

           // 结果写到 HBase
           InventoryIBCF.resultWriteHBase(inventoryId.toString(),result.toString())

           result
        }).take(inventoryIdsRddCount)
**/

    }


}