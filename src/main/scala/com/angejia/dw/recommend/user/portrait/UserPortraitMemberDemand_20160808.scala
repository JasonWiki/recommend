package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

/**
 * 用户需求单处理 (打分逻辑，同样的区域板块只会打分一次)
 * 1. 根据 userId 获取用户最新的选房单
 * 2. 对比 mysql 中的选房单, 与 hbase 中的选房单
 * 3. 把 mysql 新增的部分标签, 加分到标签中
 * 4. 把新的寻求更新到 hbase 中
 */

object UserPortraitMemberDemand_20160808 {

    val actionName = "MemberDemand"

    // 当前处理的 userId
    var userId: String = new String()
    def setUserId(userId: String): Unit = {
        this.userId = userId
    }
    def getUserId(): String = {
        this.userId
    }

    def run(): String = {
        // 推荐状态
        var reStatus = "no"

        // 最新用户需求
        val newUserDemand = this.getNewUserDemand()
        if (newUserDemand.isEmpty) return reStatus

        // 上一次需求
        val userDemand = this.getUserDemand()
        // 上一次需求不为空 && 新录入的需求与上一次需求相同
        if (!userDemand.isEmpty && newUserDemand.equals(userDemand) == true) {
            // 表示需求没有变化
            return reStatus
        }

        println(getUserId() + ": UserPortraitMemberDemand ", "")

        this.userNeeds(newUserDemand)

        this.score(newUserDemand, userDemand)

        // 把新需求更新到 dimension:userDemand 维度字段中
        this.updateUserDemand(newUserDemand)

        reStatus = "yes"
        reStatus
    }

    /**
     * 把用户的需求单, 合并到 needs 中
     */
    def userNeeds(newUserDemand: Map[String, Object]): Unit = {
        if (!newUserDemand.isEmpty) {
            val cityId = newUserDemand.getOrElse("cityId", "").toString()
            val districtIds = newUserDemand.getOrElse("districtId", "").toString().split(";")
            val blockIds = newUserDemand.getOrElse("blockId", "").toString().split(";")
            val bedrooms = newUserDemand.getOrElse("bedrooms", "").toString()
            // 价格区间数据
            val priceRange = newUserDemand.getOrElse("lowerPrice", "").toString() + "-" + newUserDemand.getOrElse("upperPrice", "").toString()
            val priceTier = UserPortraitCommon.priceTier.getOrElse(priceRange, "").toString()

            //println(cityId, districtIds.toBuffer, blockIds.toBuffer, bedrooms, priceRange, priceTier)

            // 保存用户行为数据中用到的标签
            val uesrActions = ListBuffer[Map[String, String]]()
            var i = 0
            districtIds.foreach { districtId =>
                val blockId = blockIds.apply(i)

                // 组合用户标签组数据
                val userTags = Map[String, String]()
                if (cityId != "") userTags.put(UserPortraitCommon.cityTagCode, cityId)
                if (districtId != "0") userTags.put(UserPortraitCommon.districtTagCode, districtId)
                if (blockId != "0") userTags.put(UserPortraitCommon.blockTagCode, blockId)
                if (bedrooms != "") userTags.put(UserPortraitCommon.bedroomsTagCode, bedrooms)
                if (priceTier != "") userTags.put(UserPortraitCommon.priceTagCode, priceTier)
                uesrActions.append(userTags)
                i += 1
            }

            //uesrActions.foreach(println)
            UserPortraitNeeds.setUserId(this.getUserId())
            // 把新的需求合并到, hbase 中已存在的标签中
            UserPortraitNeeds.userActionNeedsMergeAction(uesrActions)
        }
    }

    // 打分入口
    def score(newUserDemand: Map[String, Object], userDemand: Map[String, Object]): Unit = {
        // 根据需求为标签打分
        this.tagsScore(newUserDemand, userDemand)
    }

    /**
     * 最新用户需求数据
     */
    def getNewUserDemand(): Map[String, Object] = {
        var rs: Map[String, Object] = Map[String, Object]()

        val sql = "SELECT user_id,city_id,district_id,block_id,bedrooms,lower_price,upper_price " +
            "FROM member_demand " +
            "WHERE user_id = " + this.getUserId() + ";"

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
                "upperPrice" -> userDemand.get("upper_price").get.toString())
        }

        rs
    }

    /**
     * 从 Hbase 获取 选房单 维度的需求, ps : 就是上一次录入的需求
     */
    def getUserDemand(): Map[String, Object] = {

        // 结果
        var rs: Map[String, Object] = Map[String, Object]()

        // 获取用户维度数据
        val userDimension: HashMap[String, String] = UserPortraitCommon.getUserPortraitDimByUserId(this.getUserId())

        // 获取用户选房单维度数据 jsonString
        val dimUserDemandJsonStr = UserPortraitCommon.mapKeyDefaultValue(userDimension, "userDemand", "{}")
        //println(dimUserDemandJsonStr)

        // 转换 jsonStr 成 Map
        rs = UserPortraitCommon.jsonStrToMap(dimUserDemandJsonStr)

        rs
    }

    /**
     *  把需求转换成 Json 保存在
     *  用户画像表的 dimension:userDemand 中
     */
    def updateUserDemand(newUserDemand: Map[String, Object]) = {
        if (!newUserDemand.isEmpty) {
            // Map 转换为 Json Str
            val toString = UserPortraitCommon.mapToJsonStr(newUserDemand)
            UserPortraitCommon.userPortraitTable.insert(this.getUserId(), UserPortraitCommon.DimColumnFamily, "userDemand", toString)
        }
    }

    /**
     * 为标签打分
     */
    def tagsScore(newUserDemand: Map[String, Object], userDemand: Map[String, Object]): Unit = {
        // 最新需求属性
        var newCityIds: Set[String] = Set[String]()
        var newDistrictIds: Set[String] = Set[String]()
        var newBlockIds: Set[String] = Set[String]()
        var newBedrooms: Set[String] = Set[String]()
        var newLowerPrice: Set[String] = Set[String]() // 最小价格
        var newUpperPrice: Set[String] = Set[String]() // 最大价格
        var newPriceRange: String = new String()
        if (!newUserDemand.isEmpty) {
            newCityIds = newUserDemand.get("cityId").get.toString().split(";").toSet
            newDistrictIds = newUserDemand.get("districtId").get.toString().split(";").toSet
            newBlockIds = newUserDemand.get("blockId").get.toString().split(";").toSet
            newBedrooms = newUserDemand.get("bedrooms").get.toString().split(";").toSet
            // 价格区间数据
            newPriceRange = newUserDemand.get("lowerPrice").get.toString() + "-" + newUserDemand.get("upperPrice").get.toString()
        }

        // 原始需求属性
        var cityIds: Set[String] = Set[String]()
        var districtIds: Set[String] = Set[String]()
        var blockIds: Set[String] = Set[String]()
        var bedrooms: Set[String] = Set[String]()
        var lowerPrice: Set[String] = Set[String]() // 最小价格
        var upperPrice: Set[String] = Set[String]() // 最大价格
        var priceRange: String = new String()
        if (!userDemand.isEmpty) {
            cityIds = userDemand.get("cityId").get.toString().split(";").toSet
            districtIds = userDemand.get("districtId").get.toString().split(";").toSet
            blockIds = userDemand.get("blockId").get.toString().split(";").toSet
            bedrooms = userDemand.get("bedrooms").get.toString().split(";").toSet
            lowerPrice = userDemand.get("lowerPrice").get.toString().split(";").toSet
            upperPrice = userDemand.get("upperPrice").get.toString().split(";").toSet
            // 价格区间数据
            priceRange = userDemand.get("lowerPrice").get.toString() + "-" + userDemand.get("upperPrice").get.toString()
        }

        // 标签配置！ 
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

}
