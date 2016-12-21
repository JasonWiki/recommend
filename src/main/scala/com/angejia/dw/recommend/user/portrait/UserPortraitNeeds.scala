package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.control._

import com.angejia.dw.recommend.inventory.portrait.MarketingInventoryPortrait
import com.angejia.dw.common.util.JsonUtil
import com.angejia.dw.recommend.inventory.portrait.InventoryPortraitCommon

/**
 * 用户需求标签合并处理类
 */
object UserPortraitNeeds {

    /**
     * 操作的用户 id
     */
    var userId: String = new String()
    def setUserId(userId: String): Unit = {
        this.userId = userId
    }
    def getUserId(): String = {
        this.userId
    }

    /**
     * 用户 Needs 列族下的所有列的数据
     */
    var needs: HashMap[String, String] = HashMap[String, String]()
    // 初始化 needs 列族数据
    def setNeeds(): Unit = {
        this.needs = UserPortraitCommon.getUserPortraitNeedsByUserId(this.getUserId())
    }
    def getNeeds(): HashMap[String, String] = {
        this.needs
    }

    /**
     * 用户 actionNeeds 列的数据
     * 初始化 needs:actionNeeds 列数据
     * return
     *     Map[自增key, Map[标签 Code, 标签 Id]]
     */
    var actionNeeds: Map[String, Map[String, String]] = Map[String, Map[String, String]]()
    def setActionNeeds(): Unit = {
        this.setNeeds()

        // 1. 读取 Hbase 中已存在的标签数据 json String, 转化成为 Map
        val userNeedsJson = UserPortraitCommon.mapKeyDefaultValue(this.getNeeds(), "actionNeeds", "{}") // 获取行为结果列的 json 数据

        // 把 hbase 读取的 json 转换为可变 map 
        val userNeedsBaseData = JsonUtil.playJsonToMap(userNeedsJson) // 返回的是一个 Map[String, Object]
        /**
         * 这段转换写了好长时间，不要问为什么，好蛋疼.........
         */
        val userNeedsBaseDataFormat = userNeedsBaseData.map {
            case (k, v) =>
                val curK = k
                // 把元祖 v 转换为 Map[String,String]
                val curV = v.asInstanceOf[scala.collection.immutable.Map[String, String]]
                // 再把 map 转换为可变 Map
                val formatV = scala.collection.mutable.Map(curV.toSeq: _*)
                k -> formatV
        }
        // hbase 可变 map 数据
        val userNeedsBase = collection.mutable.Map(userNeedsBaseDataFormat.toSeq: _*).asInstanceOf[scala.collection.mutable.Map[String, Map[String, String]]]

        this.actionNeeds = userNeedsBase
    }
    def getActionNeeds(): Map[String, Map[String, String]] = {
        this.actionNeeds
    }

    // 共同出现次数
    val cnt: String = "cnt"

    /**
     * 用户需求合并后，保存在 hbase 中
     * userNeedsAction : 用户标签队列
     */
    def userActionNeedsMergeAction(userNeedsAction: ListBuffer[Map[String, String]]): Unit = {

        // 合并逻辑处理
        val actionNeedsMergeData = this.userActionNeedsMerge(userNeedsAction)

        // 保存到 hbase 
        this.saveActionNeeds(actionNeedsMergeData)
    }

    /**
     * 把输入的标签，Merge 到 hbase needs:actionNeeds 中已保存的标签
     * userNeedsActionList: 录入的一组新的标签组
     */
    def userActionNeedsMerge(userNeedsActionList: ListBuffer[Map[String, String]]): Map[String, Map[String, String]] = {
        // 初始化数据
        this.setActionNeeds()

        var result = Map[String, Map[String, String]]()

        // 标签集合中， map 中值为空标签顾虑掉
        val userNeedsActionFilter = userNeedsActionList.map(f => this.userTagsFilter(f))

        if (userNeedsActionFilter.isEmpty || this.getUserId().isEmpty()) return result

        // 读取 hbase needs:actionNeeds 中已经存在的数据
        var userNeedsBase: Map[String, Map[String, String]] = this.getActionNeeds()

        // 匹配 2 个 map 是否完全相同
        var tagsCompare = (map1: Map[String, String], map2: Map[String, String]) => {
            // 剔除出无用字段
            val map1Cnt = map1.getOrElse(cnt, "0")
            val map2Cnt = map2.getOrElse(cnt, "0")
            map1.remove(cnt)
            map2.remove(cnt)

            // 再进行匹配 2 个 map 是否完全一致
            val rs = map1.equals(map2)
            //println(rs, map1, map2)

            // 恢复字段
            map1.put(cnt, map1Cnt)
            map2.put(cnt, map2Cnt)
            rs
        }

        val outer = new Breaks;
        for (userNeed <- userNeedsActionFilter) {

            // 如果为空, 初始化一条记录
            if (userNeedsBase.isEmpty) {
                userNeedsBase.put("0", userNeed)
            }

            var checkStatus = false
            var curBaseUserKey = new String()
            outer.breakable {
                userNeedsBase.foreach(f => {
                    val baseUserKey = f._1 // hbase 中的需求 k
                    val baseUserNeed = f._2
                    //println(baseUserNeed)

                    // 是否相同
                    val isSame = tagsCompare(userNeed, baseUserNeed)
                    if (isSame == true) {
                        checkStatus = isSame
                        curBaseUserKey = baseUserKey
                        outer.break()
                    }

                })
            }

            if (checkStatus == true) {
                // 相同的累加次数
                val curNeedsData = userNeedsBase.getOrElse(curBaseUserKey, Map[String, String]())
                val curCnt = curNeedsData.getOrElse(cnt, "0").toInt + 1
                curNeedsData.put(cnt, curCnt.toString())
                userNeedsBase.update(curBaseUserKey, curNeedsData)
            } else {
                // 不相同的需求追加
                val newKey = userNeedsBase.size.toString()
                userNeed.put(cnt, "1")
                userNeedsBase.put(newKey, userNeed)
            }
            //println(checkStatus, userNeedsBase)

        }

        //println("rs:-----")
        //userNeedsBase.foreach(println)

        result = userNeedsBase.asInstanceOf[Map[String, Map[String, String]]]
        result
    }

    /**
     * 保存 actionNeeds 到 Hbase
     */
    def saveActionNeeds(actionNeeds: Map[String, Map[String, String]]): Unit = {
        val actionNeedsFormat = actionNeeds.map {
            case (k, v) => k -> v.toMap // 转换成不可变 Map
        }.toMap
        // 转换为 json 字符串
        val actionNeedsFormatJson = JsonUtil.playMapToJson(actionNeedsFormat)
        // 更新数据
        UserPortraitCommon.userPortraitTable.update(this.getUserId(), UserPortraitCommon.NeedsColumnFamily, "actionNeeds", actionNeedsFormatJson)
    }

    /**
     * 排序标签, 根据表现的指定 field
     * userNeedsAction 用户标签
     */
    def sortUserNeedsByField(actionNeeds: Map[String, Map[String, String]], field: String, orderBy: String = "desc"): List[(String, Map[String, String])] = {
        // 转换待排序
        val userNeedsActionBase: List[(String, Map[String, String])] = actionNeeds.toList.sortBy {
            case (k, v) => v.getOrElse(field, 1).toString().toInt
        }

        // 排序
        var userNeedsActionSort: List[(String, Map[String, String])] = List[(String, Map[String, String])]();
        orderBy match {
            case "desc" => userNeedsActionSort = userNeedsActionBase.reverse
            case "asc" => // 小伙子等你补上
        }
        userNeedsActionSort
    }

    /**
     * 用户标签过滤
     * userTags : 用户标签组
     */
    def userTagsFilter(userTags: Map[String, String]): Map[String, String] = {
        val userTagsFilter = userTags.filter { case (k, v) => !v.isEmpty() }
        userTagsFilter
    }

    /**
     * 为一组标签合并数据,通过安个家房源 Ids
     */
    def userNeedsMergeByInventoryIds(inventoryIds: Array[String]): Unit = {
        val userActions = ListBuffer[Map[String, String]]()

        inventoryIds.foreach { inventoryId =>
            // 用户标签与房源属性的 mapping
            val userTagsMappming = InventoryPortraitCommon.getUserTagsInventoryMappingByInventoryId(inventoryId)
            userActions.append(userTagsMappming)
        }

        // 对标签动作进行累加
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userActionNeedsMergeAction(userActions)
    }

    /**
     * 为一组标签合并数据,通过营销房源 Ids
     */
    def userNeedsMergeByMarketingInventoryIds(inventoryIds: Array[String]): Unit = {
        val uesrActions = ListBuffer[Map[String, String]]()

        inventoryIds.foreach { inventoryId =>
            // 用户标签与房源属性的 mapping
            val uesrTagsMappming = MarketingInventoryPortrait.getUserTagsInventoryMappingByInventoryId(inventoryId)
            uesrActions.append(uesrTagsMappming)
        }

        // 对标签动作进行累加
        UserPortraitNeeds.setUserId(this.getUserId())
        UserPortraitNeeds.userActionNeedsMergeAction(uesrActions)
    }
}
