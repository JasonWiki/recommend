package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

/**
 * 用户需求单处理
 * 1. 查询 Mysql 的需求单与 HBase
 */

object UserPortraitMemberDemand {

    val actionName = "MemberDemand"

    val userPortraitAttenuation = new UserPortraitAttenuation()

    // 当前处理的 userId
    var userId: String = new String()
    def setUserId(userId: String): Unit = {
        this.userId = userId
    }
    def getUserId(): String = {
        this.userId
    }

    // 当前处理用户的 cityId
    var cityId: String = new String()
    def setCityId(cityId: String): Unit = {
        this.cityId = cityId
    }
    def getCityId(): String = {
        this.cityId
    }

    def run(): String = {
        // 推荐状态
        var reStatus = "no"

        // Mysql 最新用户需求
        val newMemberDemand = this.getNewMemberDemand()
        if (newMemberDemand.isEmpty) return reStatus

        // HBase 保存的用户需求
        val memberDemand = this.getMemberDemand()

        // 当前需求单的修改时间
        val curUserDemandUpTime = newMemberDemand.getOrElse("updated_at", "")
        // 当前城市的用户需求最后一次修改时间
        val lastUserDemandUpTime = memberDemand.getOrElse(this.getCityId(), Map[String, Object]())
            .getOrElse("updated_at", "")

        // 若 2 次没有变化, 则什么都不做直接返回
        if (curUserDemandUpTime == lastUserDemandUpTime) {
            return reStatus
        }

        println(getUserId() + " -> " + getCityId(), ": UserPortraitMemberDemand")

        // 若修改了需求, 则把当前用户和当前城市下的所有标签衰减 99% 
        userPortraitAttenuation.setCityId(this.getCityId())
        userPortraitAttenuation.setUserId(this.getUserId())
        userPortraitAttenuation.userActionNeedsAndTagsAttenuation(1.0)

        // 根据用户需求生成新的标签组
        val uesrNeeds = this.userNeedsGenerateByMemberDemand(newMemberDemand)
        // 把新的标签组合并到, hbase 中已存在的标签中
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userActionNeedsMergeAction(uesrNeeds)

        // 根据用户的需求为标签打分
        this.newMemberDemandScore(newMemberDemand)

        // 更新需求单到 HBase
        this.updateMemberDemand(newMemberDemand, memberDemand);

        reStatus = "yes"
        reStatus
    }

    /**
     * 获取 Mysql 最新用户需求单
     */
    def getNewMemberDemand(): Map[String, Object] = {
        var rs: Map[String, Object] = Map[String, Object]()

        val sql = "SELECT user_id,city_id,district_id,block_id,bedrooms,lower_price,upper_price,updated_at " +
            "FROM angejia.member_demand " +
            "WHERE user_id = " + this.getUserId() +
            " AND city_id = " + this.getCityId() +
            ";"
        val userDemandArr = UserPortraitCommon.mysqlClient.select(sql)

        if (!userDemandArr.isEmpty) {
            val userDemand = userDemandArr(0)
            rs = Map(
                "userId" -> userDemand.get("user_id").get.toString(),
                "cityId" -> userDemand.get("city_id").get.toString(),
                "districtId" -> userDemand.get("district_id").get.toString(),
                "blockId" -> userDemand.get("block_id").get.toString(),
                "bedrooms" -> userDemand.get("bedrooms").get.toString(),
                "lowerPrice" -> userDemand.get("lower_price").get.toString(),
                "upperPrice" -> userDemand.get("upper_price").get.toString(),
                "updated_at" -> userDemand.get("updated_at").get.toString())
        }

        rs
    }

    /**
     * 从 HBase 获取当前用户所有城市的需求数据
     * ps : 就是上一次录入的需求
     * return:
     *  Map (
     *   "城市id" => Map("标签 key" => "标签 val"),
     *   "城市id" => Map("标签 key" => "标签 val"),
     *  )
     */
    def getMemberDemand(): Map[String, Map[String, Object]] = {
        // 获取用户维度数据
        val userDimension: HashMap[String, String] = UserPortraitCommon.getUserPortraitDimByUserId(this.getUserId())

        // 获取用户选房单维度数据 jsonString
        val dimUserDemandJsonStr = UserPortraitCommon.mapKeyDefaultValue(userDimension, "memberDemand", "{}")

        var rs: Map[String, Map[String, Object]] = UserPortraitCommon.jsonStrToMapByTwolayers(dimUserDemandJsonStr);

        rs
    }

    /**
     * 根据用户需求单, 生成 userNeeds 需求标签组
     */
    def userNeedsGenerateByMemberDemand(newMemberDemand: Map[String, Object]): ListBuffer[Map[String, String]] = {
        // 保存用户行为数据中用到的标签
        val uesrNeeds = ListBuffer[Map[String, String]]()

        if (!newMemberDemand.isEmpty) {
            val cityId = newMemberDemand.getOrElse("cityId", "").toString()
            val districtIds = newMemberDemand.getOrElse("districtId", "").toString().split(";") // 区域组
            val blockIds = newMemberDemand.getOrElse("blockId", "").toString().split(";") // 板块组
            val bedrooms = newMemberDemand.getOrElse("bedrooms", "").toString()
            // 根据价格,获取价格区间
            val priceRange = newMemberDemand.getOrElse("lowerPrice", "").toString() + "-" + newMemberDemand.getOrElse("upperPrice", "").toString()
            val priceTier = UserPortraitCommon.priceTier.getOrElse(priceRange, "").toString()

            //println(cityId, districtIds.toBuffer, blockIds.toBuffer, bedrooms, priceRange, priceTier)

            var i = 0 // 同区域下的版块的索引位置
            // 为区域生成版块数据
            districtIds.foreach { districtId =>
                // 当前区域下的版块
                val blockId = blockIds.apply(i)

                // 当需求的版块为空时, 找到当前区域下的所有版块, 生成标签
                if (blockId == "0" && blockId != "") {
                    // 查找当前区域下的版块
                    val blocks = UserPortraitTags.generatBlockByDistrict(Array(districtId))
                    // 组合需求呈标签
                    blocks.foreach { blockInfo =>
                        val userTags = Map[String, String]()

                        val curCityId = blockInfo.getOrElse(UserPortraitCommon.cityTagCode, "").toString()
                        val curDistrictId = blockInfo.getOrElse(UserPortraitCommon.districtTagCode, "").toString()
                        val curBlockId = blockInfo.getOrElse(UserPortraitCommon.blockTagCode, "").toString()

                        if (curCityId != "") userTags.put(UserPortraitCommon.cityTagCode, curCityId)
                        if (curDistrictId != "") userTags.put(UserPortraitCommon.districtTagCode, curDistrictId)
                        if (curBlockId != "") userTags.put(UserPortraitCommon.blockTagCode, curBlockId)
                        if (bedrooms != "") userTags.put(UserPortraitCommon.bedroomsTagCode, bedrooms)
                        if (priceTier != "") userTags.put(UserPortraitCommon.priceTagCode, priceTier)
                        uesrNeeds.append(userTags)
                    }

                    // 若需求的版块存在时, 则使用用户填写的版块
                } else {
                    val userTags = Map[String, String]()
                    if (cityId != "") userTags.put(UserPortraitCommon.cityTagCode, cityId)
                    if (districtId != "") userTags.put(UserPortraitCommon.districtTagCode, districtId)
                    if (blockId != "") userTags.put(UserPortraitCommon.blockTagCode, blockId)
                    if (bedrooms != "") userTags.put(UserPortraitCommon.bedroomsTagCode, bedrooms)
                    if (priceTier != "") userTags.put(UserPortraitCommon.priceTagCode, priceTier)
                    uesrNeeds.append(userTags)
                }

                i += 1
            }

        }

        //uesrNeeds.foreach(println)
        uesrNeeds
    }

    // 为新用户需求单中出现的标签打分
    def newMemberDemandScore(newMemberDemand: Map[String, Object]): Unit = {
        // 最新需求属性
        var newCityIds: Set[String] = Set[String]()
        var newDistrictIds: Array[String] = Array[String]()
        var newBlockIds: Array[String] = Array[String]()
        var newBedrooms: Set[String] = Set[String]()
        var newLowerPrice: Set[String] = Set[String]() // 最小价格
        var newUpperPrice: Set[String] = Set[String]() // 最大价格
        var newPriceRange: String = new String()
        if (!newMemberDemand.isEmpty) {
            newCityIds = newMemberDemand.get("cityId").get.toString().split(";").toSet
            newDistrictIds = newMemberDemand.get("districtId").get.toString().split(";")
            newBlockIds = newMemberDemand.get("blockId").get.toString().split(";")
            newBedrooms = newMemberDemand.get("bedrooms").get.toString().split(";").toSet
            // 价格区间数据
            newPriceRange = newMemberDemand.get("lowerPrice").get.toString() + "-" + newMemberDemand.get("upperPrice").get.toString()
        }
        // 价格区间段
        val newPriceTier = UserPortraitCommon.priceTier.getOrElse(newPriceRange, "").toString()

        var blockIds: ArrayBuffer[String] = new ArrayBuffer[String]()
        var i = 0 // 同区域下的版块的索引位置
        // 区域下的版块
        newDistrictIds.foreach { districtId =>
            // 当前区域下的版块
            val blockId = newBlockIds.apply(i)
            //println(districtId, blockId)

            // 当需求的版块为空时, 找到当前区域下的所有版块, 生成标签
            if (blockId == "0" && blockId != "") {
                // 查找当前区域下的版块
                val blocks = UserPortraitTags.generatBlockByDistrict(Array(districtId))

                blocks.foreach { blockInfo =>
                    val curBlockId = blockInfo.getOrElse(UserPortraitCommon.blockTagCode, "0")
                    blockIds.append(curBlockId)
                }
            } else {
                blockIds.append(blockId)
            }
            i += 1
        }

        /**
         * 为新需求打分
         */
        // 设置操作标签的用户
        UserPortraitTags.setUserId(this.getUserId())

        // 城市标签打分
        UserPortraitTags.cityTag(newCityIds, Set[String](), UserPortraitCommon.cityTagConf.getOrElse("userDemandScore", "0"))

        // 区域标签打分
        UserPortraitTags.districtTag(newDistrictIds.toSet, Set[String](), UserPortraitCommon.districtTagConf.getOrElse("userDemandScore", "0"))

        // 版块标签打分
        UserPortraitTags.blockTag(blockIds.toSet, Set[String](), UserPortraitCommon.blockTagConf.getOrElse("userDemandScore", "0"))

        // 户型标签打分
        UserPortraitTags.bedroomsTag(newBedrooms, Set[String](), UserPortraitCommon.bedroomsTagConf.getOrElse("userDemandScore", "0").toString())

        // 价格段标签打分
        UserPortraitTags.priceTag(Set(newPriceTier), Set[String](), UserPortraitCommon.priceTagConf.getOrElse("userDemandScore", "0").toString())
    }

    /**
     * 为标签打分
     */
    def tagsScore(newMemberDemand: Map[String, Object], memberDemand: Map[String, Object]): Unit = {

        // 最新需求属性
        var newCityIds: Set[String] = Set[String]()
        var newDistrictIds: Set[String] = Set[String]()
        var newBlockIds: Set[String] = Set[String]()
        var newBedrooms: Set[String] = Set[String]()
        var newLowerPrice: Set[String] = Set[String]() // 最小价格
        var newUpperPrice: Set[String] = Set[String]() // 最大价格
        var newPriceRange: String = new String()
        if (!newMemberDemand.isEmpty) {
            newCityIds = newMemberDemand.get("cityId").get.toString().split(";").toSet
            newDistrictIds = newMemberDemand.get("districtId").get.toString().split(";").toSet
            newBlockIds = newMemberDemand.get("blockId").get.toString().split(";").toSet
            newBedrooms = newMemberDemand.get("bedrooms").get.toString().split(";").toSet
            // 价格区间数据
            newPriceRange = newMemberDemand.get("lowerPrice").get.toString() + "-" + newMemberDemand.get("upperPrice").get.toString()
        }

        // 原始需求属性
        var cityIds: Set[String] = Set[String]()
        var districtIds: Set[String] = Set[String]()
        var blockIds: Set[String] = Set[String]()
        var bedrooms: Set[String] = Set[String]()
        var lowerPrice: Set[String] = Set[String]() // 最小价格
        var upperPrice: Set[String] = Set[String]() // 最大价格
        var priceRange: String = new String()
        if (!memberDemand.isEmpty) {
            cityIds = memberDemand.get("cityId").get.toString().split(";").toSet
            districtIds = memberDemand.get("districtId").get.toString().split(";").toSet
            blockIds = memberDemand.get("blockId").get.toString().split(";").toSet
            bedrooms = memberDemand.get("bedrooms").get.toString().split(";").toSet
            lowerPrice = memberDemand.get("lowerPrice").get.toString().split(";").toSet
            upperPrice = memberDemand.get("upperPrice").get.toString().split(";").toSet
            // 价格区间数据
            priceRange = memberDemand.get("lowerPrice").get.toString() + "-" + memberDemand.get("upperPrice").get.toString()
        }

        // 标签配置!
        // 标签价格段
        val newPriceTier = UserPortraitCommon.priceTier.getOrElse(newPriceRange, "").toString()
        val priceTier = UserPortraitCommon.priceTier.getOrElse(priceRange, "").toString()

        // 设置操作标签的用户
        UserPortraitTags.setUserId(this.getUserId())

        // 城市标签打分
        UserPortraitTags.cityTag(newCityIds, cityIds, UserPortraitCommon.cityTagConf.getOrElse("userDemandScore", "0"))

        // 区域标签打分, 只会打一次分 (Set(5, 2, 7),Set(5, 2, 7),Set())
        UserPortraitTags.districtTag(newDistrictIds, districtIds, UserPortraitCommon.districtTagConf.getOrElse("userDemandScore", "0"))

        // 版块标签打分, 只会打一次分 (Set(46, 0, 172),Set(46, 0, 172),Set())
        UserPortraitTags.blockTag(newBlockIds, blockIds, UserPortraitCommon.blockTagConf.getOrElse("userDemandScore", "0"))

        // 户型标签打分
        UserPortraitTags.bedroomsTag(newBedrooms, bedrooms, UserPortraitCommon.bedroomsTagConf.getOrElse("userDemandScore", "0").toString())

        // 价格段标签打分
        UserPortraitTags.priceTag(Set(newPriceTier), Set(priceTier), UserPortraitCommon.priceTagConf.getOrElse("userDemandScore", "0").toString())

    }

    /**
     *  把需求转换成 Json 保存在
     *  HBase 用户画像表的 dimension:memberDemand 中
     */
    def updateMemberDemand(
        newMemberDemand: Map[String, Object],
        memberDemand: Map[String, Map[String, Object]]) = {
        if (!newMemberDemand.isEmpty) {
            // 修改当前城市的下需求单
            memberDemand.update(this.getCityId(), newMemberDemand)
            //println(memberDemand)

            // Map 转换为 Json Str
            val toString = UserPortraitCommon.mapToJsonStrByTwolayers(memberDemand)
            //println(toString)

            // 保存到 HBase 中
            UserPortraitCommon.userPortraitTable.insert(this.getUserId(), UserPortraitCommon.DimColumnFamily, "memberDemand", toString)
        }
    }

}
