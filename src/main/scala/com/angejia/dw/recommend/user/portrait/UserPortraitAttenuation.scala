package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

import com.angejia.dw.hadoop.hbase.HBaseClient
import com.angejia.dw.recommend.Conf

/**
 * 安个家用户画像定期衰减，入口
 */
object UserPortraitAttenuation {

    def main(args: Array[String]) {

        for (arg <- args) {
            println(arg)
        }

        // 环境 develop 开发环境， online 线上环境
        val env = args(0)

        // 用户 Id
        var userId = args(1)
        //userId = ""      // 613

        // 衰减方法
        val userPortraitAttenuation = new UserPortraitAttenuation()
        userPortraitAttenuation.init(env)
        if (userId.isEmpty()) {
            userPortraitAttenuation.userAttenuationAction()
        } else {
            userPortraitAttenuation.userAttenuationAction(userId)
        }

    }
}

/**
 * 安个家用户画像定期衰减逻辑
 */
class UserPortraitAttenuation {

    // 当前处理的 userId
    var userId: String = new String()
    def setUserId(userId: String): Unit = {
        if (userId.isEmpty()) {
            return
        }
        this.userId = userId
    }
    def getUserId(): String = {
        if (this.userId.isEmpty()) {
            return "0"
        }
        this.userId
    }

    // 城市 id
    var cityId: String = new String();
    def setCityId(cityId: String): Unit = {
        if (cityId.isEmpty()) {
            return
        }
        this.cityId = cityId
    }
    def getCityId(): String = {
        if (this.cityId.isEmpty()) {
            return "0"
        }
        this.cityId
    }

    // 当前用户下 tags 列族的标签组
    var userTags: HashMap[String, String] = HashMap[String, String]()
    def setUserTags(): Unit = {
        // 根据 uesrId 获取 tag 标签组数据
        this.userTags = UserPortraitCommon.getUserPortraitTagsByUserId(this.getUserId())
    }
    def getUserTags(): HashMap[String, String] = {
        this.userTags
    }

    /**
     * 初始化环境
     */
    def init(env: String): Unit = {
        // 初始化环境变量
        Conf.setEnv(env)

        // 初始化 Hbase 用户画像表
        UserPortraitCommon.userPortraitTable = new HBaseClient("userPortrait", Conf.getZookeeperQuorum())
    }

    /**
     * 用户衰减流程控制
     */
    def userAttenuationAction(userId: String = ""): Unit = {

        // 对单个用户进行衰减
        if (!userId.isEmpty()) {
            println("----------------------------")

            // 设置操作的用户 ID
            this.setUserId(userId)
            // tag 衰减
            this.userTagsAttenuation()
            // actionNeed 衰减
            this.userActionNeedsAttenuation()

            // 对所有用户进行衰减
        } else {
            // hbase 读取所有用户画像数据
            val scan = UserPortraitCommon.userPortraitTable.scanBase() // 获取搜索条件
            val userPortraitData = UserPortraitCommon.userPortraitTable.getTableDataByScan(scan) // 获取用户所有数据
            // 迭代器
            val iteratorRs = userPortraitData.iterator()

            // 遍历每用户所有数据
            while (iteratorRs.hasNext()) {
                val rowResult: Result = iteratorRs.next()
                val userId = Bytes.toString(rowResult.getRow)

                println("----------------------------")

                // 设置操作的用户 ID
                this.setUserId(userId)
                // tag 衰减
                this.userTagsAttenuation()
                // actionNeed 衰减
                this.userActionNeedsAttenuation()

            }
        }
    }

    /**
     * tag 列族的所有标签进行衰减
     */
    def userTagsAttenuation(): Unit = {
        println(this.getUserId(), "userTags 开始衰减")

        // 初始化数据
        this.setUserTags()

        // 城市衰减
        this.attenuationTag(UserPortraitCommon.cityTagCode)

        // 区域衰减
        this.attenuationTag(UserPortraitCommon.districtTagCode)

        // 版块衰减
        this.attenuationTag(UserPortraitCommon.blockTagCode)

        // 小区衰减
        this.attenuationTag(UserPortraitCommon.communityTagCode)

        // 户型衰减
        this.attenuationTag(UserPortraitCommon.bedroomsTagCode)

        // 价格衰减
        this.attenuationTag(UserPortraitCommon.priceTagCode)
    }

    /**
     * 衰减指定 tag 标签
     */
    def attenuationTag(tagCode: String): Unit = {

        // 获取配置的衰减百分比
        var attenuationPercentage: Double = 0

        tagCode match {
            // 城市标签衰减百分比
            case UserPortraitCommon.cityTagCode => {
                attenuationPercentage = UserPortraitCommon.cityTagConf.getOrElse("attenuationPercentage", "0.01").toDouble
            }
            // 区域标签衰减百分比
            case UserPortraitCommon.districtTagCode => {
                attenuationPercentage = UserPortraitCommon.districtTagConf.getOrElse("attenuationPercentage", "0.01").toDouble
            }
            // 版块标签衰减百分比
            case UserPortraitCommon.blockTagCode => {
                attenuationPercentage = UserPortraitCommon.blockTagConf.getOrElse("attenuationPercentage", "0.01").toDouble
            }
            // 小区标签衰减百分比
            case UserPortraitCommon.communityTagCode => {
                attenuationPercentage = UserPortraitCommon.communityTagConf.getOrElse("attenuationPercentage", "0.01").toDouble
            }
            // 户型标签衰减百分比
            case UserPortraitCommon.bedroomsTagCode => {
                attenuationPercentage = UserPortraitCommon.bedroomsTagConf.getOrElse("attenuationPercentage", "0.01").toString().toDouble
            }
            // 价格标签衰减百分比
            case UserPortraitCommon.priceTagCode => {
                attenuationPercentage = UserPortraitCommon.priceTagConf.getOrElse("attenuationPercentage", "0.01").toString().toDouble
            }
            case _ => {
                println("衰减标签百分比,不在匹配范围中")
                return
            }
        }

        if (attenuationPercentage != 0) {
            // 获取对应标签
            val tag = UserPortraitCommon.mapKeyDefaultValue(this.getUserTags(), tagCode, "{}")
            // 标签衰减后的数据
            val rs = UserPortraitTags.tagAttenuationScore(tag, attenuationPercentage)
            // 把衰减后的数据写回 Hbase
            if (!rs.isEmpty()) UserPortraitCommon.userPortraitTable.update(this.getUserId(), UserPortraitCommon.TagColumnFamily, tagCode, rs)

        }
    }

    /**
     * 为 needs:actionNeeds 的 cnt 次数进行衰减
     */
    def userActionNeedsAttenuation(): Unit = {
        println(this.getUserId(), "userActionNeeds 开始衰减")

        // 执行衰减
        val userNeeds = this.actionNeedsAttenuation(0.1)

        // 保存衰减后的数据
        UserPortraitNeeds.saveActionNeeds(userNeeds)
    }

    /**
     * 为 actionNeeds 的 cnt 次数进行衰减
     * scorePercentage : 衰减的比例, 0.1 表示只衰减原来的 10 % 的次数
     */
    def actionNeedsAttenuation(scorePercentage: Double): Map[String, Map[String, String]] = {
        // 获取用户画像需求标签数据
        UserPortraitNeeds.setUserId(this.getUserId())

        // 初始化数据
        UserPortraitNeeds.setNeeds()
        UserPortraitNeeds.setActionNeeds()

        // 获取所有标签组数据
        val actionNeeds = UserPortraitNeeds.getActionNeeds()

        // 对标签组出现的次数进行衰减 
        actionNeeds.map {
            case (tagIndex, tags) =>

                // 当前标签组的次数
                val cntTagCnt = tags.getOrElse(UserPortraitNeeds.cnt, "0").toInt

                // 衰减后的次数
                val attenuationTagCnt = cntTagCnt - (cntTagCnt * scorePercentage)

                // 分数向下取整 -> 转换为 int -> 转换为字符串返回
                tags.update(UserPortraitNeeds.cnt, attenuationTagCnt.floor.toInt.toString())
        }
        actionNeeds
    }

    /**
     * 衰减 user 不同城市下 的所有标签组 和 标签分数
     */
    def userActionNeedsAndTagsAttenuation(scorePercentage: Double) {
        // 获取用户画像需求标签数据
        UserPortraitNeeds.setUserId(this.getUserId())

        // 初始化数据
        UserPortraitNeeds.setNeeds()
        UserPortraitNeeds.setActionNeeds()

        // 获取所有标签组数据
        var actionNeeds = UserPortraitNeeds.getActionNeeds()

        // 对标签组出现的次数进行衰减
        actionNeeds.map {
            case (tagIndex, tags) =>
                //println(tags, "-----")

                // 当前标签里的标签 id
                val tagCityId = tags.getOrElse(UserPortraitCommon.cityTagCode, "0")
                val tagDistrictId = tags.getOrElse(UserPortraitCommon.districtTagCode, "0")
                val tagBlockId = tags.getOrElse(UserPortraitCommon.blockTagCode, "0")
                val tagCommunityId = tags.getOrElse(UserPortraitCommon.communityTagCode, "0")
                val tagBedroomsId = tags.getOrElse(UserPortraitCommon.bedroomsTagCode, "0")
                val tagPriceId = tags.getOrElse(UserPortraitCommon.priceTagCode, "0")

                //println(tagCityId, tagDistrictId, tagBlockId, tagCommunityId, tagBedroomsId, tagPriceId)

                // 需要衰减的城市
                if (tagCityId == this.getCityId()) {

                    /**
                     * 衰减标签组的次数
                     */
                    val cntTagCnt = tags.getOrElse(UserPortraitNeeds.cnt, "0").toInt // 当前标签组的次数
                    val attenuationTagCnt = cntTagCnt - (cntTagCnt * scorePercentage) // 衰减后的次数
                    tags.update(UserPortraitNeeds.cnt, attenuationTagCnt.floor.toInt.toString()) // 分数向下取整 -> 转换为 int -> 转换为字符串返回

                    /**
                     * 衰减标签组中指定标签 id 的分数
                     */
                    this.tagAttenuationScoreByTagIdAction(UserPortraitCommon.cityTagCode, tagCityId, scorePercentage)
                    this.tagAttenuationScoreByTagIdAction(UserPortraitCommon.districtTagCode, tagDistrictId, scorePercentage)
                    this.tagAttenuationScoreByTagIdAction(UserPortraitCommon.blockTagCode, tagBlockId, scorePercentage)
                    this.tagAttenuationScoreByTagIdAction(UserPortraitCommon.communityTagCode, tagCommunityId, scorePercentage)
                    this.tagAttenuationScoreByTagIdAction(UserPortraitCommon.bedroomsTagCode, tagBedroomsId, scorePercentage)
                    this.tagAttenuationScoreByTagIdAction(UserPortraitCommon.priceTagCode, tagPriceId, scorePercentage)

                }
            //println("")
        }

        // 保存到 HBase
        UserPortraitNeeds.saveActionNeeds(actionNeeds)
    }

    /**
     * tagCode: 标签代码 , city, district 等
     * tagId: 标签id
     * scorePercentage: 标签 id 衰减比例
     */
    def tagAttenuationScoreByTagIdAction(tagCode: String, tagId: String, scorePercentage: Double) {
        this.setUserTags()
        // 所有标签的分数
        val tagsScore = this.getUserTags()
        // 衰减前
        val tagScoreJsonStr = UserPortraitCommon.mapKeyDefaultValue(tagsScore, tagCode, "{}")
        // 衰减后
        val tagScoreJsonStrAttenuation = UserPortraitTags.tagAttenuationScoreByTagId(tagScoreJsonStr, tagId, scorePercentage)

        //println(tagId, tagScoreJsonStr, tagScoreJsonStrAttenuation)

        // 写入 HBase
        if (!tagScoreJsonStrAttenuation.isEmpty())
            UserPortraitCommon.userPortraitTable.update(
                this.getUserId(),
                UserPortraitCommon.TagColumnFamily,
                tagCode, tagScoreJsonStrAttenuation)
    }

}