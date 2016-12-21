package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

import com.angejia.dw.common.util.JsonUtil
import com.angejia.dw.common.util.RegexUtil

/**
 * 筛选房源逻辑处理
 * 根据 URL - 找出 城市，区域，板块，户型，价格区间(转换成价格段)
 * 1. 收集 url 出现的以上标签,
 * 比如: val cityIds = Set(1)
 * 2. 读取 Hbase 中的以上标签原始 Json 数据
 * 3. 对找出的标签进行 筛选逻辑加分后写回 Hbase
 */
object UserPortraitFilter {

    val actionName = "Filter"

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

    /**
     * 本行为中用到的用户标签
     */
    var userTags = Map[String, String]()

    // URI 搜索匹配正则
    val filterRegex = "/mobile/member/inventories/list[?](.*)"

    def run(): String = {
        // 推荐状态
        var reStatus = "no"

        /**
         * 解析出 Url 中的筛选字段数据
         */
        val urlPars = RegexUtil.findStrData(this.filterRegex, this.getRequestUri())
        if (urlPars.isEmpty()) return reStatus
        println(getUserId() + ": UserPortraitFilter ", this.getRequestUri())

        this.userTags = Map[String, String](
            UserPortraitCommon.cityTagCode -> new String(),
            UserPortraitCommon.districtTagCode -> new String(),
            UserPortraitCommon.blockTagCode -> new String(),
            UserPortraitCommon.communityTagCode -> new String(),
            UserPortraitCommon.bedroomsTagCode -> new String(),
            UserPortraitCommon.priceTagCode -> new String())

        /**
         * 处理 url 中出现的标签
         */
        urlPars.split("&").foreach { keyValueStr =>

            val keyValue = keyValueStr.split("=")
            // 表示一对 key,value
            if (keyValue.size == 2) {

                val key: String = keyValue(0).toString()
                val value: String = keyValue(1).toString()

                key match {
                    case "city_id" => {
                        userTags.update(UserPortraitCommon.cityTagCode, value)
                    }
                    case "district_id" => {
                        userTags.update(UserPortraitCommon.districtTagCode, value)
                    }
                    case "block_id" => {
                        userTags.update(UserPortraitCommon.blockTagCode, value)
                    }
                    case "community_id" => {
                        userTags.update(UserPortraitCommon.communityTagCode, value)
                    }
                    case "bedroom_id" => {
                        // 通过户型 key 找到实际户型
                        val bedrooms = UserPortraitCommon.bedroomsType.getOrElse(value, "0").toString()
                        userTags.update(UserPortraitCommon.bedroomsTagCode, bedrooms)
                    }
                    case "price_id" => {
                        // 通过价格段 key 找到实际的户型
                        val priceTierId = UserPortraitCommon.priceTierType.getOrElse(value, "0").toString()
                        userTags.update(UserPortraitCommon.priceTagCode, priceTierId)
                    }
                    case _ => "filter nothing"
                }

            }
        }

        this.userNeeds()

        this.score()

        reStatus = "yes"
        reStatus
    }

    /**
     * 一组标签进行合并
     */
    def userNeeds(): Unit = {
        val uesrActions = ListBuffer[Map[String, String]]()
        uesrActions.append(userTags)

        // 对标签动作进行累加
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userActionNeedsMergeAction(uesrActions)
    }

    /**
     * 标签打分
     */
    def score(): Unit = {

        // 设置操作标签的用户
        UserPortraitTags.setUserId(this.getUserId())

        // 城市标签打分
        val cityId = userTags.getOrElse(UserPortraitCommon.cityTagCode, "0")
        UserPortraitTags.cityTag(Set(cityId), Set(), UserPortraitCommon.cityTagConf.getOrElse("filterScore", "0"))

        // 区域标签打分
        val districtId = userTags.getOrElse(UserPortraitCommon.districtTagCode, "0")
        UserPortraitTags.districtTag(Set(districtId), Set(), UserPortraitCommon.districtTagConf.getOrElse("filterScore", "0"))

        // 版块标签打分
        val blockId = userTags.getOrElse(UserPortraitCommon.blockTagCode, "0")
        UserPortraitTags.blockTag(Set(blockId), Set(), UserPortraitCommon.blockTagConf.getOrElse("filterScore", "0"))

        // 小区标签打分
        val communityId = userTags.getOrElse(UserPortraitCommon.communityTagCode, "0")
        UserPortraitTags.communityTag(Set(communityId), Set(), UserPortraitCommon.communityTagConf.getOrElse("filterScore", "0"))

        // 户型标签打分
        val bedrooms = userTags.getOrElse(UserPortraitCommon.bedroomsTagCode, "0")
        UserPortraitTags.bedroomsTag(Set(bedrooms), Set(), UserPortraitCommon.bedroomsTagConf.getOrElse("filterScore", "0").toString())

        // 价格段标签打分
        val priceTierId = userTags.getOrElse(UserPortraitCommon.priceTagCode, "0")
        UserPortraitTags.priceTag(Set(priceTierId), Set(), UserPortraitCommon.priceTagConf.getOrElse("filterScore", "0").toString())
    }

}