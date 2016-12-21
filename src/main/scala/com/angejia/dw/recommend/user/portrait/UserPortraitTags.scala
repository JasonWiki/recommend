package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import com.angejia.dw.common.util.JsonUtil
import com.angejia.dw.recommend.inventory.portrait.InventoryPortraitCommon
import com.angejia.dw.recommend.inventory.portrait.MarketingInventoryPortrait

/**
 * 用户标签核心类
 */
object UserPortraitTags {

    // 保存当前用户所有标签数据
    var userTags: HashMap[String, String] = HashMap[String, String]()

    var userId: String = new String()
    def setUserId(userId: String): Unit = {
        this.userId = userId

        // 获取用户相关的标签数据
        userTags = UserPortraitCommon.getUserPortraitTagsByUserId(userId)
    }
    def getUserId(): String = {
        this.userId
    }

    /**
     * 获取用户 tag 数据
     */
    def getUserTagByTagCode(tagCode: String): Map[String, String] = {
        val TagJsonStr = UserPortraitCommon.mapKeyDefaultValue(userTags, tagCode, "{}")

        // 把 hbase 读取的 json 转换为可变 map 
        val userTagData = JsonUtil.playJsonToMap(TagJsonStr) // 返回的是一个 Map[String, Object]
        val rs = scala.collection.mutable.Map(userTagData.toSeq: _*).asInstanceOf[Map[String, String]]
        rs
    }

    // 城市标签逻辑处理
    def cityTag(newCityIds: Set[String], cityIds: Set[String], tagScore: String): Unit = {
        // 标签 json String 数据
        val cityTagJsonStr = UserPortraitCommon.mapKeyDefaultValue(userTags, UserPortraitCommon.cityTagCode, "{}")
        // 标签 分数
        //val cityUserDemandScore = UserPortraitCommon.cityTagConf.getOrElse("userDemandScore","0")
        val cityUserDemandScore = tagScore
        // 加分
        this.tagScoreAction(
            this.getUserId(),
            UserPortraitCommon.TagColumnFamily,
            cityTagJsonStr,
            newCityIds,
            cityIds,
            UserPortraitCommon.cityTagCode,
            cityUserDemandScore)
    }

    // 区域标签逻辑处理
    def districtTag(newDistrictIds: Set[String], districtIds: Set[String], tagScore: String): Unit = {
        // 标签 json String 数据
        val districtTagJsonStr = UserPortraitCommon.mapKeyDefaultValue(userTags, UserPortraitCommon.districtTagCode, "{}")
        // 标签 分数
        //val districtUserDemandScore = UserPortraitCommon.districtTagConf.getOrElse("userDemandScore","0")
        val districtUserDemandScore = tagScore
        // 加分
        this.tagScoreAction(
            this.getUserId(),
            UserPortraitCommon.TagColumnFamily,
            districtTagJsonStr,
            newDistrictIds,
            districtIds,
            UserPortraitCommon.districtTagCode,
            districtUserDemandScore)
    }

    // 版块标签逻辑处理
    def blockTag(newBlockIds: Set[String], blockIds: Set[String], tagScore: String): Unit = {
        // 标签 json String 数据
        val blockTagJsonStr = UserPortraitCommon.mapKeyDefaultValue(userTags, UserPortraitCommon.blockTagCode, "{}")
        // 标签 分数
        //val blockUserDemandScore = UserPortraitCommon.blockTagConf.getOrElse("userDemandScore", "0")
        val blockUserDemandScore = tagScore
        // 加分
        this.tagScoreAction(
            this.getUserId(),
            UserPortraitCommon.TagColumnFamily,
            blockTagJsonStr,
            newBlockIds,
            blockIds,
            UserPortraitCommon.blockTagCode,
            blockUserDemandScore)
    }

    // 小区标签逻辑处理
    def communityTag(newCommunityIds: Set[String], communityIds: Set[String], tagScore: String): Unit = {
        // 标签 json String 数据
        val communityTagJsonStr = UserPortraitCommon.mapKeyDefaultValue(userTags, UserPortraitCommon.communityTagCode, "{}")
        // 标签 分数
        //val communityUserDemandScore = UserPortraitCommon.communityTagConf.getOrElse("browseScore", "0").toString()
        val communityUserDemandScore = tagScore
        // 加分
        this.tagScoreAction(
            this.getUserId(),
            UserPortraitCommon.TagColumnFamily,
            communityTagJsonStr,
            newCommunityIds,
            communityIds,
            UserPortraitCommon.communityTagCode,
            communityUserDemandScore)
    }

    // 户型标签逻辑处理
    def bedroomsTag(newBedrooms: Set[String], bedrooms: Set[String], tagScore: String): Unit = {
        // 标签 json String 数据
        val bedroomsTagJsonStr = UserPortraitCommon.mapKeyDefaultValue(userTags, UserPortraitCommon.bedroomsTagCode, "{}")
        // 标签 分数
        //val bedroomsUserDemandScore = UserPortraitCommon.bedroomsTagConf.getOrElse("userDemandScore", "0")
        val bedroomsUserDemandScore = tagScore
        // 加分
        this.tagScoreAction(
            this.getUserId(),
            UserPortraitCommon.TagColumnFamily,
            bedroomsTagJsonStr,
            newBedrooms,
            bedrooms,
            UserPortraitCommon.bedroomsTagCode,
            bedroomsUserDemandScore)
    }

    // 价格标签逻辑处理
    def priceTag(newPriceTierIds: Set[String], priceTierIds: Set[String], tagScore: String): Unit = {
        // 标签 json String 数据
        val priceTagJsonStr = UserPortraitCommon.mapKeyDefaultValue(userTags, UserPortraitCommon.priceTagCode, "{}")
        // 标签 分数
        //val priceUserDemandScore = UserPortraitCommon.priceTagConf.getOrElse("userDemandScore", "0").toString()
        val priceUserDemandScore = tagScore
        // 加分
        this.tagScoreAction(
            this.getUserId(),
            UserPortraitCommon.TagColumnFamily,
            priceTagJsonStr,
            newPriceTierIds,
            priceTierIds,
            UserPortraitCommon.priceTagCode,
            priceUserDemandScore)
    }

    /**
     * 通过房源 Id 为标签打分
     */
    def tagScoreByInventoryIdAndScore(inventoryId: String, tagScore: String): Unit = {
        /**
         * 等待加分的标签
         */
        var cityIds: Set[String] = Set[String]()
        var districtIds: Set[String] = Set[String]()
        var blockIds: Set[String] = Set[String]()
        var communityIds: Set[String] = Set[String]()
        var bedrooms: Set[String] = Set[String]()
        var priceTierIds: Set[String] = Set[String]()

        // 房源数据
        val inventoryData = InventoryPortraitCommon.getUserTagsInventoryMappingByInventoryId(inventoryId)
        //println(inventoryId, inventoryData)

        if (!inventoryData.isEmpty) {
            cityIds = Set(inventoryData.getOrElse(UserPortraitCommon.cityTagCode, "0"))
            districtIds = Set(inventoryData.getOrElse(UserPortraitCommon.districtTagCode, "0"))
            blockIds = Set(inventoryData.getOrElse(UserPortraitCommon.blockTagCode, "0"))
            communityIds = Set(inventoryData.getOrElse(UserPortraitCommon.communityTagCode, "0"))
            bedrooms = Set(inventoryData.getOrElse(UserPortraitCommon.bedroomsTagCode, "0"))
            priceTierIds = Set(UserPortraitCommon.priceTagCode)
        }
        //println(cityIds, districtIds, blockIds, communityIds, bedrooms, priceTierIds)

        // 设置操作标签的用户
        UserPortraitTags.setUserId(this.getUserId())

        // 城市标签打分
        UserPortraitTags.cityTag(cityIds, Set(), tagScore)

        // 区域标签打分
        UserPortraitTags.districtTag(districtIds, Set(), tagScore)

        // 版块标签打分
        UserPortraitTags.blockTag(blockIds, Set(), tagScore)

        // 小区标签打分
        UserPortraitTags.communityTag(communityIds, Set(), tagScore)

        // 户型标签打分
        UserPortraitTags.bedroomsTag(bedrooms, Set(), tagScore)

        // 价格段标签打分
        UserPortraitTags.priceTag(priceTierIds, Set(), tagScore)
    }

    private def scoreByInventoryData(inventoryData: Map[String, String], actionName: String): Unit = {
        /**
         * 等待加分的标签
         */
        var cityIds: Set[String] = Set[String]()
        var districtIds: Set[String] = Set[String]()
        var blockIds: Set[String] = Set[String]()
        var communityIds: Set[String] = Set[String]()
        var bedrooms: Set[String] = Set[String]()
        var priceTierIds: Set[String] = Set[String]()

        // 当前行为, 各个标签分数
        var cityScore = "0"
        var districtScore = "0"
        var blockScore = "0"
        var communityScore = "0"
        var bedroomsScore = "0"
        var priceScore = "0"

        /**
         * 根据不同的行为, 打不同的分数
         */
        actionName match {
            // 选房单行为
            case UserPortraitMemberDemand.actionName => {
                cityScore = UserPortraitCommon.cityTagConf.getOrElse("userDemandScore", "0").toString()
                districtScore = UserPortraitCommon.districtTagConf.getOrElse("userDemandScore", "0").toString()
                blockScore = UserPortraitCommon.blockTagConf.getOrElse("userDemandScore", "0").toString()
                communityScore = UserPortraitCommon.communityTagConf.getOrElse("userDemandScore", "0").toString()
                bedroomsScore = UserPortraitCommon.bedroomsTagConf.getOrElse("userDemandScore", "0").toString()
                priceScore = UserPortraitCommon.priceTagConf.getOrElse("userDemandScore", "0").toString()
            }
            // 筛选行为
            case UserPortraitFilter.actionName => {
                cityScore = UserPortraitCommon.cityTagConf.getOrElse("filterScore", "0").toString()
                districtScore = UserPortraitCommon.districtTagConf.getOrElse("filterScore", "0").toString()
                blockScore = UserPortraitCommon.blockTagConf.getOrElse("filterScore", "0").toString()
                communityScore = UserPortraitCommon.communityTagConf.getOrElse("filterScore", "0").toString()
                bedroomsScore = UserPortraitCommon.bedroomsTagConf.getOrElse("filterScore", "0").toString()
                priceScore = UserPortraitCommon.priceTagConf.getOrElse("filterScore", "0").toString()
            }
            // 浏览行为
            case UserPortraitBrowse.actionName => {
                cityScore = UserPortraitCommon.cityTagConf.getOrElse("browseScore", "0").toString()
                districtScore = UserPortraitCommon.districtTagConf.getOrElse("browseScore", "0").toString()
                blockScore = UserPortraitCommon.blockTagConf.getOrElse("browseScore", "0").toString()
                communityScore = UserPortraitCommon.communityTagConf.getOrElse("browseScore", "0").toString()
                bedroomsScore = UserPortraitCommon.bedroomsTagConf.getOrElse("browseScore", "0").toString()
                priceScore = UserPortraitCommon.priceTagConf.getOrElse("browseScore", "0").toString()
            }
            // 收藏行为
            case UserPortraitLikeInventory.actionName => {
                cityScore = UserPortraitCommon.cityTagConf.getOrElse("likeInventoryScore", "0").toString()
                districtScore = UserPortraitCommon.districtTagConf.getOrElse("likeInventoryScore", "0").toString()
                blockScore = UserPortraitCommon.blockTagConf.getOrElse("likeInventoryScore", "0").toString()
                communityScore = UserPortraitCommon.communityTagConf.getOrElse("likeInventoryScore", "0").toString()
                bedroomsScore = UserPortraitCommon.bedroomsTagConf.getOrElse("likeInventoryScore", "0").toString()
                priceScore = UserPortraitCommon.priceTagConf.getOrElse("likeInventoryScore", "0").toString()
            }
            // 带看行为
            case UserPortraitVisitItem.actionName => {
                cityScore = UserPortraitCommon.cityTagConf.getOrElse("visitItemInventoryScore", "0").toString()
                districtScore = UserPortraitCommon.districtTagConf.getOrElse("visitItemInventoryScore", "0").toString()
                blockScore = UserPortraitCommon.blockTagConf.getOrElse("visitItemInventoryScore", "0").toString()
                communityScore = UserPortraitCommon.communityTagConf.getOrElse("visitItemInventoryScore", "0").toString()
                bedroomsScore = UserPortraitCommon.bedroomsTagConf.getOrElse("visitItemInventoryScore", "0").toString()
                priceScore = UserPortraitCommon.priceTagConf.getOrElse("visitItemInventoryScore", "0").toString()
            }
            // 发生过连接的行为
            case UserPortraitLinkInventory.actionName => {
                cityScore = UserPortraitCommon.cityTagConf.getOrElse("linkInventoryScore", "0").toString()
                districtScore = UserPortraitCommon.districtTagConf.getOrElse("linkInventoryScore", "0").toString()
                blockScore = UserPortraitCommon.blockTagConf.getOrElse("linkInventoryScore", "0").toString()
                communityScore = UserPortraitCommon.communityTagConf.getOrElse("linkInventoryScore", "0").toString()
                bedroomsScore = UserPortraitCommon.bedroomsTagConf.getOrElse("linkInventoryScore", "0").toString()
                priceScore = UserPortraitCommon.priceTagConf.getOrElse("linkInventoryScore", "0").toString()
            }
            case _ => println("未知动作")
        }

        if (!inventoryData.isEmpty) {
            cityIds = Set(inventoryData.getOrElse(UserPortraitCommon.cityTagCode, "0"))
            districtIds = Set(inventoryData.getOrElse(UserPortraitCommon.districtTagCode, "0"))
            blockIds = Set(inventoryData.getOrElse(UserPortraitCommon.blockTagCode, "0"))
            communityIds = Set(inventoryData.getOrElse(UserPortraitCommon.communityTagCode, "0"))
            bedrooms = Set(inventoryData.getOrElse(UserPortraitCommon.bedroomsTagCode, "0"))
            priceTierIds = Set(inventoryData.getOrElse(UserPortraitCommon.priceTagCode, "0"))
        }
        //println(cityIds, districtIds, blockIds, communityIds, bedrooms, priceTierIds)

        // 设置操作标签的用户
        UserPortraitTags.setUserId(this.getUserId())

        // 城市标签打分
        UserPortraitTags.cityTag(cityIds, Set(), cityScore)

        // 区域标签打分
        UserPortraitTags.districtTag(districtIds, Set(), districtScore)

        // 版块标签打分
        UserPortraitTags.blockTag(blockIds, Set(), blockScore)

        // 小区标签打分
        UserPortraitTags.communityTag(communityIds, Set(), communityScore)

        // 户型标签打分
        UserPortraitTags.bedroomsTag(bedrooms, Set(), bedroomsScore)

        // 价格段标签打分
        UserPortraitTags.priceTag(priceTierIds, Set(), priceScore)
    }

    /**
     * 根据不同行为标签打分
     */
    @deprecated("use scoreByInventoryIdAndAction and scoreByMarketingInventoryIdAndAction","2016-11-14")
    def tagsScoreByInventoryAndAction(inventoryId: String, actionName: String): Unit = {
        val inventoryData = InventoryPortraitCommon.getUserTagsInventoryMappingByInventoryId(inventoryId);
        this.scoreByInventoryData(inventoryData, actionName)
    }

    /**
     * 根据不同行为标签打分(安个家房源)
     */
    def scoreByInventoryIdAndAction(inventoryId: String, actionName: String): Unit = {
        val inventoryData = InventoryPortraitCommon.getUserTagsInventoryMappingByInventoryId(inventoryId);
        this.scoreByInventoryData(inventoryData, actionName)
    }

    /**
     * 根据不同行为标签打分（营销房源）
     */
    def scoreByMarketingInventoryIdAndAction(inventoryId: String, actionName: String): Unit = {
        val inventoryData = MarketingInventoryPortrait.getUserTagsInventoryMappingByInventoryId(inventoryId);
        this.scoreByInventoryData(inventoryData, actionName)
    }

    /**
     * 为一个 Tag 加分逻辑处理动作
     */
    def tagScoreAction(
        rowKeyId: String,
        columnFamily: String,
        tagJsonStr: String,
        newIds: Set[String],
        ids: Set[String],
        tagCode: String,
        tagCodeScore: String): Unit = {

        // 源标签 json 格式数据
        val curTagJsonStr = tagJsonStr

        // 新标签 Ids 和 原标签 Ids 的差集
        val curIdsDiff = newIds -- ids
        //println(newIds, ids, curIdsDiff)

        // 标签代码
        val curTagCode = tagCode

        // 标签需要加的分数
        val curTagCodeScore = tagCodeScore

        // 为变更的标签, 计算得出加分后的 Json 格式数据
        val curJsonStrRs = this.jsonTagAddScore(curTagJsonStr, curIdsDiff, curTagCodeScore)
        //println(curJsonStrRs)

        // 加好分数, 更新到原始标签中
        if (!curJsonStrRs.isEmpty()) UserPortraitCommon.userPortraitTable.insert(rowKeyId, columnFamily, curTagCode, curJsonStrRs)
    }

    /**
     * 为指定标签加分
     * jsonStrTag 需要加分的 Json 格式标签
     *    例如： {"21":"10","24":"10","23":"10","22":"10","25":"20"}
     * addScoreField 需要加分的字段 key
     *    例如   Set("21", "24")
     * score 加的分数
     *    例如   10
     */
    def jsonTagAddScore(jsonStrTag: String, addScoreField: Set[String], score: String): String = {
        var rs = Map[String, Object]()

        // 如果不为空, 则把 jsonStrTag 字符串转化成为 Map
        if (!jsonStrTag.isEmpty()) {
            rs = UserPortraitCommon.jsonStrToMap(jsonStrTag).asInstanceOf[Map[String, Object]]
        }

        // 加分
        addScoreField.foreach { field =>
            if (!field.isEmpty() && !field.equals("0")) {
                val baseFieldScore = rs.getOrElse(field, 0).toString().toInt
                rs.put(field, (baseFieldScore + score.toInt).toString())
            }
        }

        //println(jsonStrTag, addScoreField, rs)
        // Map 转换为 JsonStr
        if (rs.isEmpty) "" else UserPortraitCommon.mapToJsonStr(rs)
    }

    /**
     * 为指定标签按照比例衰减分数
     * jsonStrTag 操作的 Json 格式标签
     *    例如： {"21":"10","24":"10","23":"10","22":"10","25":"20"}
     * scorePercentage 按照原分数衰减的百分比
     *    例如   0.1 表示按照原来值的 10 % 去衰减
     */
    def tagAttenuationScore(jsonStrTag: String, scorePercentage: Double): String = {
        var rs = Map[String, Object]()

        // 如果不为空, 则把 jsonStrTag 字符串转化成为 Map
        if (!jsonStrTag.isEmpty()) {
            rs = UserPortraitCommon.jsonStrToMap(jsonStrTag).asInstanceOf[Map[String, Object]]

            // 衰减
            rs.foreach {
                case (k, v) =>
                    val tagId = k
                    var tagScore = v.toString().toInt

                    // 衰减后的分数
                    val attenuationScore = tagScore - (tagScore * scorePercentage)

                    // 分数向下取整 -> 转换为 int -> 转换为字符串返回
                    rs.update(tagId, attenuationScore.floor.toInt.toString())
            }
        }

        if (rs.isEmpty) "" else UserPortraitCommon.mapToJsonStr(rs)
    }

    /**
     * 衰减一组标签中, 指定 id 的分数
     *  例如
     *   jsonStrTag =  {"21":"10","24":"10","23":"10","22":"10","25":"20"}
     *   tagId = 21
     *   scorePercentage = 0.90
     *  结果:
     *    {"21":"1","24":"10","23":"10","22":"10","25":"20"}
     */
    def tagAttenuationScoreByTagId(jsonStrTag: String, tagId: String, scorePercentage: Double): String = {
        // 获取对应标签的分数
        val tagScoreMap = UserPortraitCommon.jsonStrToMap(jsonStrTag).asInstanceOf[Map[String, Object]]
        var tagScore = tagScoreMap.getOrElse(tagId, "0").toString().toInt
        // 衰减后的分数
        val attenuationScore = tagScore - (tagScore * scorePercentage)
        tagScoreMap.update(tagId, attenuationScore.floor.toInt.toString())
        UserPortraitCommon.mapToJsonStr(tagScoreMap)
    }

    /**
     * 排序标签,按照分数,从大到小
     */
    def sortUserTagByScore(tagCode: String): List[(String, String)] = {
        val curTagJsonStr = UserPortraitCommon.mapKeyDefaultValue(userTags, tagCode, "{}")
        var tagMap = Map[String, String]()
        if (!curTagJsonStr.isEmpty()) {
            // 把 json 转换成为 map
            tagMap = UserPortraitCommon.jsonStrToMap(curTagJsonStr).asInstanceOf[Map[String, String]]
        }
        var rs = ""
        // 按照标签分数排序
        val sort = tagMap.toList.sortBy(kv => kv._2.toInt).reverse
        sort
    }

    /**
     *  获得价格段 - 根据价格值
     */
    def getPriceTier(price: String): String = {
        var rs: String = ""
        val priceLong = price.toLong

        for ((k, v) <- UserPortraitCommon.priceTier) {
            // 价格段数据
            val curPriceRange = k.toString().split("-")
            val startPrice = curPriceRange(0).toLong
            val endPrice = curPriceRange(1).toLong
            val curPriceTier = v
            if (priceLong >= startPrice && priceLong < endPrice) {
                rs = v
            }
        }
        rs
    }

    /**
     * 获取价格区间 - 根据价格值
     */
    def getPriceRangeByPrice(price: String): (Long, Long) = {
        var rs: (Long, Long) = null

        val priceLong = price.toLong // 价格

        for ((k, v) <- UserPortraitCommon.priceTier) {
            // 价格段数据
            val curPriceRange = k.toString().split("-")
            val startPrice = curPriceRange(0).toLong
            val endPrice = curPriceRange(1).toLong

            if (priceLong >= startPrice && priceLong < endPrice) {
                rs = (startPrice, endPrice)
            }
        }
        rs
    }

    /**
     * 获取价格区间 - 根据价格段
     */
    def getPriceRangeByTier(tier: String): (Long, Long) = {
        var rs: (Long, Long) = null

        for ((k, v) <- UserPortraitCommon.priceTier) {
            if (v == tier) {
                // 价格段数据
                val curPriceRange = k.toString().split("-")
                val startPrice = curPriceRange(0).toLong
                val endPrice = curPriceRange(1).toLong
                rs = (startPrice, endPrice)
            }
        }
        rs
    }

    /**
     * 获取价格段 - 根据价格区间
     */
    def getPriceTierByPriceRange(start: String, end: String): String = {
        val priceRange = start.toString() + "-" + end.toString()
        val priceTier = UserPortraitCommon.priceTier.getOrElse(priceRange, "").toString()
        priceTier
    }

    /**
     * 根据区域 ids 获取 版块列表
     * 例如: val ids = Array("1","2")
     */
    def generatBlockByDistrict(districtIds: Array[String]): ListBuffer[Map[String, String]] = {
        val blockList = ListBuffer[Map[String, String]]()

        districtIds.foreach { districtId =>
            // 查找当前区域下的所有版块
            val sql = "SELECT id,city_id,district_id " +
                "FROM angejia.block " +
                "WHERE district_id = " + districtId +
                ";"
            // 当前区域下的所有版块
            val disBlocks = UserPortraitCommon.mysqlClient.select(sql)

            // 生成最终标签
            disBlocks.foreach { curBlock =>
                val tags = Map[String, String]()
                val curCityId = curBlock.getOrElse("city_id", "").toString()
                val curDistrictId = curBlock.getOrElse("district_id", "").toString()
                val curBlockId = curBlock.getOrElse("id", "").toString()

                tags.put(UserPortraitCommon.cityTagCode, curCityId)
                tags.put(UserPortraitCommon.districtTagCode, curDistrictId)
                tags.put(UserPortraitCommon.blockTagCode, curBlockId)

                blockList.append(tags)
            }
        }

        blockList
    }

}