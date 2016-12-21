package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

import com.angejia.dw.common.util.DateUtil
import com.angejia.dw.common.util.RegexUtil

/**
 * 浏览房源数据行为
 * 根据 URL 找出 房源 Id
 * 1. 查找 Hbase 房源 ID 的，城市，区域，板块，小区，户型，所属的价格区间段
 * 2. 读取 Hbase 中的以上标签原始 Json 数据
 * 3. 对找出的标签进行 浏览加分后写回 Hbase
 */
object UserPortraitBrowse {

    val actionName = "Browse"

    // 当前处理的 userId
    var userId: String = new String()

    def setUserId(userId: String): Unit = {
        this.userId = userId
    }

    def getUserId(): String = {
        this.userId
    }

    // 请求的 URI
    var requestUri = new String()
    def setRequestUri(uri: String): Unit = {
        this.requestUri = uri
    }
    def getRequestUri(): String = {
        this.requestUri
    }

    var inventoryId = new String()

    // 房源单页 uri 正则匹配
    val browseRegex = """^/mobile/member/(?:inventories|inventory/detail)/\d+/(\d+)"""

    /**
     * 本行为中用到的用户标签
     */
    var userTags = Map[String, String]()

    def run(): String = {
        // 推荐状态
        var reStatus = "no"

        // 清空
        this.inventoryId = ""

        /**
         * 解析出 Url 中的article id
         */
        val articleId = RegexUtil.findStrData(this.browseRegex, this.getRequestUri())

        if (articleId.isEmpty() || articleId == "") return reStatus

        /*
         * 根据article获取inventory id和resource
         * 若resource为1，为安个家二手房
         * 若resource为2，则为营销房源
         */
        val sql = "SELECT inventory_id,resource " +
            "FROM angejia.article " +
            "WHERE id = " + articleId
        val article: ArrayBuffer[HashMap[String, Any]] = UserPortraitCommon.mysqlClient.select(sql)

        if (article.length != 1) {
            return reStatus
        }

        inventoryId = article(0).getOrElse("inventory_id", "").toString
        val resource = article(0).getOrElse("resource", "").toString

        this.userTags = Map[String, String](
            UserPortraitCommon.cityTagCode -> new String(),
            UserPortraitCommon.districtTagCode -> new String(),
            UserPortraitCommon.blockTagCode -> new String(),
            UserPortraitCommon.communityTagCode -> new String(),
            UserPortraitCommon.bedroomsTagCode -> new String(),
            UserPortraitCommon.priceTagCode -> new String())

        if (resource == "1") {
            // 安个家房源
            println(DateUtil.getCurTime(DateUtil.SIMPLE_FORMAT) + "|"
                + getUserId() + ": UserPortraitBrowse", inventoryId, this.getRequestUri(), "angejia")
            this.updateUserNeedsByInventoryId(inventoryId)
            this.scoreByInventoryId(inventoryId)
        } else {
            // 营销房源
            println(DateUtil.getCurTime(DateUtil.SIMPLE_FORMAT) + "|"
                + getUserId() + ": UserPortraitBrowse", inventoryId, this.getRequestUri(), "marketing")
            this.updateUserNeedsByMarketingInventoryId(inventoryId)
            this.scoreByMarketingInventoryId(inventoryId)
        }

        reStatus = "yes"
        reStatus
    }

    /**
     * 一组标签进行合并（安个家房源）
     */
    private def updateUserNeedsByInventoryId(inventoryId: String): Unit = {
        val inventoryIds = Array(inventoryId)

        // 合并
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userNeedsMergeByInventoryIds(inventoryIds)
    }

    /**
     * 一组标签进行合并（营销房源）
     */
    private def updateUserNeedsByMarketingInventoryId(inventoryId: String): Unit = {
        val inventoryIds = Array(inventoryId)

        // 合并
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userNeedsMergeByMarketingInventoryIds(inventoryIds)
    }

    /**
     * 打分入口（安个家房源）
     */
    def scoreByInventoryId(inventoryId: String): Unit = {
        UserPortraitTags.setUserId(this.getUserId())
        UserPortraitTags.scoreByInventoryIdAndAction(inventoryId, this.actionName)
    }

    /**
     * 打分入口（营销房源）
     */
    def scoreByMarketingInventoryId(inventoryId: String): Unit = {
        UserPortraitTags.setUserId(this.getUserId())
        UserPortraitTags.scoreByMarketingInventoryIdAndAction(inventoryId, this.actionName)
    }
}
