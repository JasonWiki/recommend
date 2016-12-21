package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import com.angejia.dw.common.util.JsonUtil

/**
 * 用户画像，建模状态
 */
object UserPortraitrModelState {

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

    /**
     * 用户 modelState 列族下的所有列的数据
     */
    var modelState: HashMap[String, String] = HashMap[String, String]()
    def setModelState(): Unit = {
        this.modelState = UserPortraitCommon.getUserPortraitModelStateByUserId(this.getUserId())
    }
    def getModelState(): HashMap[String, String] = {
        this.modelState
    }

    /**
     * 用户 visitItemInventorysRecord 带看房源推荐记录
     * 初始化 modelState:visitItemInventorysRecord 列数据
     * return
     *     Map[String, Map[String, String]]
     */
    var visitItemInventorysRecord: Map[String, Map[String, String]] = Map[String, Map[String, String]]()

    def setVisitItemInventorysRecord(): Unit = {
        this.setModelState()

        // 读取 Hbase 中已存在的标签数据 json String
        val jsonString = UserPortraitCommon.mapKeyDefaultValue(this.getModelState(), "visitItemInventorysRecord", "{}")

        // 转化成为可变的 Map
        this.visitItemInventorysRecord = this.toolJsonStringToChangeMap(jsonString)
    }

    def getVisitItemInventorysRecord(): Map[String, Map[String, String]] = {
        this.visitItemInventorysRecord
    }

    /**
     * 保存 visitItemInventorysRecord 结果到 Hbase 中
     */
    def saveVisitItemInventorysRecord(visitItemInventorysRecord: Map[String, Map[String, String]]): String = {
        // 更新数据
        this.toolSaveMapDataToHbaseColumn("visitItemInventorysRecord", visitItemInventorysRecord)
    }

    /**
     * 用户 linkInventorysRecord 连接房源记录
     * 初始化 modelState:linkInventorysRecord 列数据
     * return
     *     Map[String, Map[String, String]]
     */
    var linkInventorysRecord: Map[String, Map[String, String]] = Map[String, Map[String, String]]()

    def setLinkInventorysRecord(): Unit = {
        this.setModelState()

        // 读取 Hbase 中已存在的标签数据 json String
        val jsonString = UserPortraitCommon.mapKeyDefaultValue(this.getModelState(), "linkInventorysRecord", "{}")

        // 转化成为可变的 Map
        this.linkInventorysRecord = this.toolJsonStringToChangeMap(jsonString)
    }

    def getLinkInventorysRecord(): Map[String, Map[String, String]] = {
        this.linkInventorysRecord
    }

    /**
     * 保存 linkInventorysRecord 结果到 Hbase 中
     */
    def saveLinkInventorysRecord(linkInventorysRecord: Map[String, Map[String, String]]): String = {
        // 更新数据
        this.toolSaveMapDataToHbaseColumn("linkInventorysRecord", linkInventorysRecord)
    }

    /**
     * json 字符串转换为一个可变的 Map
     * return
     *     Map[String, Map[String, String]
     */
    def toolJsonStringToChangeMap(jsonStringInput: String): scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, String]] = {
        //import scala.collection.mutable.Map
        var jsonString = jsonStringInput
        if (jsonString.isEmpty() || jsonString == "") {
            jsonString = "{}"
        }

        // string 转换为 json 格式(不可变  Map)
        val mapData = JsonUtil.playJsonToMap(jsonString) // 返回的是一个 Map[String, Object]

        /**
         * 这段转换写了好长时间，不要问为什么，好蛋疼.........
         */
        // 把 json 转换为可变 map 
        val jsonToMap = mapData.map {
            case (k, v) =>
                val curK = k
                // 把元祖 v 转换为 Map[String,String]
                val curV = v.asInstanceOf[scala.collection.immutable.Map[String, String]]
                // 再把 map 转换为可变 Map
                val formatV = scala.collection.mutable.Map(curV.toSeq: _*)
                k -> formatV
        }
        // hbase 可变 map 数据
        val rs = collection.mutable.Map(jsonToMap.toSeq: _*).asInstanceOf[scala.collection.mutable.Map[String, Map[String, String]]]

        rs
    }

    /**
     * 保存 map 到数据到 hbase
     * column : UserPortraitCommon.RecommendColumnFamily 列族下的列
     * mapData : map 数据
     */
    def toolSaveMapDataToHbaseColumn(column: String, mapData: Map[String, Map[String, String]]): String = {
        val map = mapData.map {
            case (k, v) => k -> v.toMap // 转换成不可变 Map
        }.toMap
        // 转换为 json 字符串
        val jsonString = JsonUtil.playMapToJson(map)
        // 更新数据
        UserPortraitCommon.userPortraitTable.update(this.getUserId(), UserPortraitCommon.ModelStateColumnFamily, column, jsonString)
    }

}