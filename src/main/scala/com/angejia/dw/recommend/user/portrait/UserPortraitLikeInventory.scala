package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

import com.angejia.dw.common.util.DateUtil
/**
 * 收藏房源打分
 * 根据 URL - 获取房源 Id
 * 1. 查找 Hbase 房源 ID 的，城市，区域，板块，小区，户型，所属的价格区间段
 * 2. 读取 Hbase 中的以上标签原始 Json 数据
 * 3. 对找出的标签进行 浏览加分后写回 Hbase
 */
object UserPortraitLikeInventory {

    val actionName = "LikeInventory"

    // 当前处理的 userId
    var userId: String = new String()

    // 推荐状态
    var reStatus = "no"

    def setUserId(userId: String): Unit = {
        this.userId = userId
    }
    def getUserId(): String = {
        this.userId
    }

    def run(): String = {
        reStatus = "no"

        runLikeInventory()
        runLikeMarketingInventory()

        reStatus
    }

    def runLikeInventory() = {
        // 获取最新用户收藏房源
        val newLikeInventorys: Map[String, Object] = this.getNewLikeInventorys()
        if (!newLikeInventorys.isEmpty) {
            // 用户原始收藏房源
            val likeInventorys: Map[String, Object] = this.getLikeInventorys()

            // 检测新增的房源 Ids
            val diffInventoryIds = this.diffInventoryIds(newLikeInventorys, likeInventorys)

            if (!diffInventoryIds.isEmpty) {
                println(DateUtil.getCurTime(DateUtil.SIMPLE_FORMAT) + "|"
                    + getUserId() + ": UserPortraitLikeInventory ", diffInventoryIds.mkString(","), "angejia")

                // 需求标签合并
                this.updateUserNeedsByInventoryIds(diffInventoryIds)

                // 打分
                this.scoreByInventoryIds(diffInventoryIds)

                // 把新收藏房源更新到 dimension:likeInventorys 维度字段中
                this.updateLikeInventorys(newLikeInventorys)
                reStatus = "yes"
            }
        }
    }

    def runLikeMarketingInventory() = {
        // 获取最新用户收藏房源
        val newLikeInventorys: Map[String, Object] = this.getNewLikeMarketingInventorys()
        if (!newLikeInventorys.isEmpty) {
            // 用户原始收藏房源
            val likeInventorys: Map[String, Object] = this.getLikeMarketingInventorys()

            // 检测新增的房源 Ids
            val diffInventoryIds = this.diffMarketingInventoryIds(newLikeInventorys, likeInventorys)

            if (!diffInventoryIds.isEmpty) {
                println(DateUtil.getCurTime(DateUtil.SIMPLE_FORMAT) + "|"
                    + getUserId() + ": UserPortraitLikeInventory ", diffInventoryIds.mkString(","), "marketing")

                // 需求标签合并
                this.updateUserNeedsByMarketingInventoryIds(diffInventoryIds)

                // 打分
                this.scoreByMarketingInventoryIds(diffInventoryIds)

                // 把新收藏房源更新到 dimension:likeInventorys 维度字段中
                this.updateLikeMarketingInventorys(newLikeInventorys)
                reStatus = "yes"
            }
        }
    }

    /**
     * 一组标签进行合并（安个家房源）
     */
    def updateUserNeedsByInventoryIds(inventoryIds: Array[String]): Unit = {
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userNeedsMergeByInventoryIds(inventoryIds)
    }

    /**
     * 一组标签进行合并（营销房源）
     */
    def updateUserNeedsByMarketingInventoryIds(inventoryIds: Array[String]): Unit = {
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userNeedsMergeByMarketingInventoryIds(inventoryIds)
    }

    /**
     * 打分入口（安个家房源）
     */
    def scoreByInventoryIds(inventoryIds: Array[String]): Unit = {
        // 为标签打分
        inventoryIds.foreach { inventoryId =>
            UserPortraitTags.setUserId(this.getUserId())
            UserPortraitTags.scoreByInventoryIdAndAction(inventoryId, this.actionName)
        }
    }

    /**
     * 打分入口(营销房源)
     */
    def scoreByMarketingInventoryIds(inventoryIds: Array[String]): Unit = {
        // 为标签打分
        inventoryIds.foreach { inventoryId =>
            UserPortraitTags.setUserId(this.getUserId())
            UserPortraitTags.scoreByMarketingInventoryIdAndAction(inventoryId, this.actionName)
        }
    }

    /**
     * 获取用户喜欢的安个家房源
     */
    def getNewLikeInventorys(): Map[String, Object] = {
        var rs: Map[String, Object] = Map[String, Object]()

        var sql = """
            SELECT
              user_id
              ,inventory_id
              ,a_id
            FROM angejia.member_like_inventory
            WHERE
              angejia.member_like_inventory.status = 1
              AND user_id = """ + this.getUserId()
        val userLikeInventoryData: ArrayBuffer[HashMap[String, Any]] = UserPortraitCommon.mysqlClient.select(sql)

        var likeInventoryIds = new ArrayBuffer[String];
        var likeArticleIds = new ArrayBuffer[String];

        for (like <- userLikeInventoryData) {
            var a_id = like.getOrElse("a_id", 0).toString
            if (a_id == "0") {
                val inventory_id = like.getOrElse("inventory_id", "0").toString
                likeInventoryIds.append(inventory_id)
            } else {
                likeArticleIds.append(a_id)
            }
        }

        if (!likeArticleIds.isEmpty) {
            // 从article id 中解析出来inventory id
            sql = """
                SELECT inventory_id
                FROM angejia.article
                WHERE resource =1
                  AND id in (
            """ +
                likeArticleIds.mkString(",") +
                ")"
            val userLikeArticleData: ArrayBuffer[HashMap[String, Any]] = UserPortraitCommon.mysqlClient.select(sql)
            val userLikeInventoryFromArticle = for {
                i <- userLikeArticleData
                val inventoryId = i.getOrElse("inventory_id", "0").toString
            } yield inventoryId

            likeInventoryIds ++= userLikeInventoryFromArticle
        }

        // 转换成字符串, 用 ; 号分割
        if (!likeInventoryIds.isEmpty) {
            rs.put("likeInventorys", likeInventoryIds.mkString(";"))
        }

        rs
    }

    /**
     * 获取用户喜欢的营销房源
     */
    def getNewLikeMarketingInventorys(): Map[String, Object] = {
        var rs: Map[String, Object] = Map[String, Object]()

        var sql = """
            SELECT
              user_id
              ,a_id
            FROM angejia.member_like_inventory
            WHERE
              angejia.member_like_inventory.status = 1
              AND user_id = """ + this.getUserId()
        val userLikeInventoryData: ArrayBuffer[HashMap[String, Any]] = UserPortraitCommon.mysqlClient.select(sql)

        var likeArticleIds = for {
            like <- userLikeInventoryData
            val a_id = like.getOrElse("a_id", 0).toString
        } yield a_id

        var userLikeMarketingInventoryIds = new ArrayBuffer[String]
        if (!likeArticleIds.isEmpty) {
            // 从article id 中解析出来inventory id
            sql = """
            SELECT inventory_id
            FROM angejia.article
            WHERE resource =2
              AND id in (
            """ +
                likeArticleIds.mkString(",") +
                ")"
            val userLikeMarketingArticleData: ArrayBuffer[HashMap[String, Any]] = UserPortraitCommon.mysqlClient.select(sql)
            userLikeMarketingInventoryIds = for {
                i <- userLikeMarketingArticleData
                val inventoryId = i.getOrElse("inventory_id", "0").toString
            } yield inventoryId
        }

        // 转换成字符串, 用户 ; 号分割
        if (!userLikeMarketingInventoryIds.isEmpty) {
            rs.put("likeMarketingInventorys", userLikeMarketingInventoryIds.mkString(";"))
        }

        rs
    }

    /**
     * 从 Hbase 获取 喜欢房源 维度的需求
     */
    def getLikeInventorys(): Map[String, Object] = {

        // 结果
        var rs: Map[String, Object] = Map[String, Object]()

        // 获取用户维度数据
        val userDimension: HashMap[String, String] = UserPortraitCommon.getUserPortraitDimByUserId(this.getUserId())

        // 获取用户 喜欢房源维度 维度数据 jsonStri
        val dimLikeInventorysJsonStr = UserPortraitCommon.mapKeyDefaultValue(userDimension, "likeInventorys", "{}")

        // 转换 jsonStr 成 Map
        rs = UserPortraitCommon.jsonStrToMap(dimLikeInventorysJsonStr)

        rs
    }

    /**
     * 从 Hbase 获取 喜欢房源 维度的需求
     */
    def getLikeMarketingInventorys(): Map[String, Object] = {

        // 结果
        var rs: Map[String, Object] = Map[String, Object]()

        // 获取用户维度数据
        val userDimension: HashMap[String, String] = UserPortraitCommon.getUserPortraitDimByUserId(this.getUserId())

        // 获取用户 喜欢房源维度 维度数据 jsonString
        val dimLikeInventorysJsonStr = UserPortraitCommon.mapKeyDefaultValue(userDimension, "likeMarketingInventorys", "{}")

        // 转换 jsonStr 成 Map
        rs = UserPortraitCommon.jsonStrToMap(dimLikeInventorysJsonStr)

        rs
    }

    /**
     *  把用户喜欢的安个家房源保存到维度数据中
     *  保存到用户画像表的 dimension:likeInventorys 中
     */
    def updateLikeInventorys(likeInventorys: Map[String, Object]) = {
        if (!likeInventorys.isEmpty) {
            // Map 转换为 Json Str
            val toString = UserPortraitCommon.mapToJsonStr(likeInventorys)
            UserPortraitCommon.userPortraitTable.insert(this.getUserId(), UserPortraitCommon.DimColumnFamily, "likeInventorys", toString)
        }
    }

    /**
     *  把用户喜欢的营销房源保存到维度数据中
     *  保存到用户画像表的 dimension:likeMarketingInventorys 中
     */
    def updateLikeMarketingInventorys(likeInventorys: Map[String, Object]) = {
        if (!likeInventorys.isEmpty) {
            // Map 转换为 Json Str
            val toString = UserPortraitCommon.mapToJsonStr(likeInventorys)
            UserPortraitCommon.userPortraitTable.insert(this.getUserId(), UserPortraitCommon.DimColumnFamily, "likeMarketingInventorys", toString)
        }
    }

    // 获取房源差集
    def diffInventoryIds(newLikeInventorys: Map[String, Object], likeInventorys: Map[String, Object]): Array[String] = {
        var newLikeInventoryIds: Set[String] = Set[String]()
        var likeInventoryIds: Set[String] = Set[String]()

        if (!newLikeInventorys.isEmpty) {
            //newLikeInventoryIds = newLikeInventorys.get("likeInventorys").get.toString().split(";").toSet
            newLikeInventoryIds = newLikeInventorys.getOrElse("likeInventorys", "").toString().split(";").toSet
        }
        if (!likeInventorys.isEmpty) {
            //likeInventoryIds = likeInventorys.get("likeInventorys").get.toString().split(";").toSet
            likeInventoryIds = likeInventorys.getOrElse("likeInventorys", "").toString().split(";").toSet
        }

        // 差集
        val diffInventoryIds = newLikeInventoryIds -- likeInventoryIds

        diffInventoryIds.toArray
    }

    // 获取房源差集
    def diffMarketingInventoryIds(newLikeInventorys: Map[String, Object], likeInventorys: Map[String, Object]): Array[String] = {
        var newLikeInventoryIds: Set[String] = Set[String]()
        var likeInventoryIds: Set[String] = Set[String]()

        if (!newLikeInventorys.isEmpty) {
            //newLikeInventoryIds = newLikeInventorys.get("likeInventorys").get.toString().split(";").toSet
            newLikeInventoryIds = newLikeInventorys.getOrElse("likeMarketingInventorys", "").toString().split(";").toSet
        }
        if (!likeInventorys.isEmpty) {
            //likeInventoryIds = likeInventorys.get("likeInventorys").get.toString().split(";").toSet
            likeInventoryIds = likeInventorys.getOrElse("likeMarketingInventorys", "").toString().split(";").toSet
        }

        // 差集
        val diffInventoryIds = newLikeInventoryIds -- likeInventoryIds

        diffInventoryIds.toArray
    }
}
