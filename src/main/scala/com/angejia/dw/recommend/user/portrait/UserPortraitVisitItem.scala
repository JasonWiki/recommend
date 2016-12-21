package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import com.angejia.dw.common.util.DateUtil

/**
 * 用户发生过带看的房源
 * 1. 获取用户被带看过得房源
 * 2. 为带看过得房子打分
 */
object UserPortraitVisitItem {

    val actionName = "VisitItem"

    def run(userId: String, date: String): String = {
        var userPortraitVisitItem = new UserPortraitVisitItem()
        userPortraitVisitItem.setUserId(userId)
        userPortraitVisitItem.setDwDate(date)
        var reStatus = userPortraitVisitItem.run()

        userPortraitVisitItem = null
        reStatus
    }
}

/**
 * 流程
 */
class UserPortraitVisitItem {

    // hbase dimension:visitItemInventorys  列名
    val column: String = "visitItemInventorys"

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

    // 分区日期
    var dwDate: String = new String()
    def setDwDate(date: String): Unit = {
        this.dwDate = date
    }
    def getDwDate(): String = {
        this.dwDate
    }

    /**
     * 初始化环境
     */
    def run(): String = {
        // 推荐状态
        var reStatus = "no"

        // 当日日期
        val offsetDate = DateUtil.getCalendarOffsetDateDay(0) // 获取当天日期
        val todayYmd = DateUtil.DateToString(offsetDate, DateUtil.SIMPLE_Y_M_D_FORMAT) // 格式化日期

        // 当日建模建模状态
        val modelState = this.getModelStateByDate(todayYmd)
        if (modelState == true) return reStatus

        // 最新带看房源数据
        val newVisitItemIds = this.getNewVisitItemIds()
        // 如果最新带看数据为空, 则退出
        if (newVisitItemIds.isEmpty) {
            this.saveModelStateByDate(todayYmd) // 保存当日的建模状态
            return reStatus
        }

        // 原始带看房源数据
        val visitItemIds = this.getVisitItemIds()

        //  检测新增的房源 Ids
        val diffInventoryIds = this.diffInventoryIds(newVisitItemIds, visitItemIds)

        // 如果没有变化, 则退出
        if (diffInventoryIds.isEmpty) {
            this.saveModelStateByDate(todayYmd) // 保存当日的建模状态
            return reStatus
        }
        println(getUserId() + ": UserPortraitVisitItem ", diffInventoryIds.mkString(","))

        // 需求标签合并
        this.userNeeds(diffInventoryIds)

        // 打分
        this.score(diffInventoryIds)

        // 最新的需求更新到 hbase 中
        this.updateInventorysToHbase(newVisitItemIds)

        // 全部成功, 保存当日的建模状态
        this.saveModelStateByDate(todayYmd) // 保存当日的建模状态

        // 返回
        reStatus = "yes"
        reStatus
    }

    /**
     * 获取用户最新带看数据
     */
    def getNewVisitItemIds(): Map[String, Object] = {
        var rs: Map[String, Object] = Map[String, Object]()

        // 读取指定用户的带看房源数据
        val querySql = "SELECT visit_item_invs_a FROM dw_user_sd WHERE user_id = '" + this.getUserId() + "' AND p_dt = '" + this.getDwDate() + "' limit 1"
        //println(querySql)
        //println(querySql)
        val userSdData = UserPortraitCommon.sparkHiveClient.select(querySql, "visit_item_invs_a");

        if (!userSdData.isEmpty()) {
            // 所有的带看房源数据
            val visitItemIvns: String = userSdData.get(0).get("visit_item_invs_a")
            if (visitItemIvns != null) {
                rs.put(column, visitItemIvns)
            }
        }
        rs
    }

    /**
     * 从 Hbase 获取用户最新带看数据
     */
    def getVisitItemIds(): Map[String, Object] = {

        var rs: Map[String, Object] = Map[String, Object]()

        // 获取用户维度数据
        val visitItemInventorys: HashMap[String, String] = UserPortraitCommon.getUserPortraitDimByUserId(this.getUserId())

        // 获取用户 喜欢房源维度 维度数据 jsonStri
        val dimVisitItemInventorysJsonStr = UserPortraitCommon.mapKeyDefaultValue(visitItemInventorys, column, "{}")

        // 转换 jsonStr 成 Map
        rs = UserPortraitCommon.jsonStrToMap(dimVisitItemInventorysJsonStr)

        rs
    }

    // 获取房源差集
    def diffInventoryIds(newInventorys: Map[String, Object], oldInventorys: Map[String, Object]): Array[String] = {
        var newInventoryIds: Set[String] = Set[String]()
        var oldInventoryIds: Set[String] = Set[String]()

        if (!newInventorys.isEmpty) {
            newInventoryIds = newInventorys.getOrElse(column, "").toString().split(",").toSet
        }
        if (!oldInventorys.isEmpty) {
            oldInventoryIds = oldInventorys.getOrElse(column, "").toString().split(",").toSet
        }

        // 差集
        val diffInventoryIds = newInventoryIds -- oldInventoryIds

        diffInventoryIds.toArray
    }

    /**
     * 一组标签进行合并
     */
    def userNeeds(inventoryIds: Array[String]): Unit = {
        // 合并
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userNeedsMergeByInventoryIds(inventoryIds)
    }

    /**
     * 通过房源 Id, 为房源属性打分
     */
    def score(inventoryIds: Array[String]): Unit = {
        // 通过房源 ID , 为用户标签打分
        inventoryIds.foreach { inventoryId =>
            UserPortraitTags.setUserId(this.getUserId())
            // 分数
            // val score =  UserPortraitCommon.cityTagConf.getOrElse("visitItemInventoryScore", "0").toString()
            // UserPortraitTags.tagScoreByInventoryId(inventoryId, score)
            UserPortraitTags.tagsScoreByInventoryAndAction(inventoryId, UserPortraitVisitItem.actionName)
        }
    }

    /**
     *  把需求转换成 Json
     *  保存到用户画像表的 dimension:visitItemInventorys 中
     */
    def updateInventorysToHbase(inventorys: Map[String, Object]) = {
        if (!inventorys.isEmpty) {
            // Map 转换为 Json Str
            val toString = UserPortraitCommon.mapToJsonStr(inventorys)
            UserPortraitCommon.userPortraitTable.update(this.getUserId(), UserPortraitCommon.DimColumnFamily, column, toString)
        }
    }

    /**
     * 指定日期的建模状态
     * dateYmd: 日期 2016-04-10
     * return
     *  true : 已建模
     *  false : 未建模
     */
    def getModelStateByDate(dateYmd: String): Boolean = {
        var status = false
        UserPortraitrModelState.setUserId(this.getUserId())
        UserPortraitrModelState.setVisitItemInventorysRecord()
        val visitItemInventorysRecord = UserPortraitrModelState.getVisitItemInventorysRecord() // 获取指定天数是否已经建模过了
        if (visitItemInventorysRecord.contains(dateYmd)) {
            status = true
        }
        status
    }

    /**
     * 保存属性的建模状态
     * dateYmd: 日期 2016-04-10
     */
    def saveModelStateByDate(dateYmd: String): Unit = {
        // 把建模状态写入到 hbase 中
        UserPortraitrModelState.setUserId(this.getUserId())
        var newVisitItemInventorysRecord: Map[String, Map[String, String]] = Map[String, Map[String, String]]()
        newVisitItemInventorysRecord.put(dateYmd, Map("status" -> "1")) // Map[当前日期 -> Map[status->1]]
        UserPortraitrModelState.saveVisitItemInventorysRecord(newVisitItemInventorysRecord) // 更新到 Hbase
    }

}