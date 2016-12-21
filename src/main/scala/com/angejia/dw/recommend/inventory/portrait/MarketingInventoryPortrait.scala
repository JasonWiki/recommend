package com.angejia.dw.recommend.inventory.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import com.angejia.dw.hadoop.hbase.HBaseClient

import com.angejia.dw.recommend.user.portrait.UserPortraitCommon
import com.angejia.dw.recommend.user.portrait.UserPortraitTags
import com.angejia.dw.common.util.mysql.MysqlClient

object MarketingInventoryPortrait {
    /**
     * 通过房源 Id 获取房源画像基础数据
     */
    private def getInventoryPortraitByInventoryId(inventoryId: String): HashMap[String, String] = {
        var querySql = sqlStmt.format(inventoryId.toInt)
        val res = UserPortraitCommon.mysqlClient.select(querySql)
        val result = new HashMap[String, String]()
        if (!res.isEmpty) {
            for ((k, v) <- res(0)) {
                result.put(k, v.toString)
            }
        }
        result
    }

    /**
     * 通过房源 Id 获取, 获取标签code 与房源属性的 Mapping 数据
     */
    def getUserTagsInventoryMappingByInventoryId(inventoryId: String): Map[String, String] = {
        val rs = Map[String, String]()

        val inventoryPortrait = this.getInventoryPortraitByInventoryId(inventoryId)
        if (!inventoryPortrait.isEmpty) {
            val cityId = inventoryPortrait.getOrElse("city_id", "0")
            rs.put(UserPortraitCommon.cityTagCode, cityId)

            val districtId = inventoryPortrait.getOrElse("district_id", "0")
            rs.put(UserPortraitCommon.districtTagCode, districtId)

            val blockId = inventoryPortrait.getOrElse("block_id", "0")
            rs.put(UserPortraitCommon.blockTagCode, blockId)

            val communityId = inventoryPortrait.getOrElse("community_id", "0")
            rs.put(UserPortraitCommon.communityTagCode, communityId)

            val bedrooms = inventoryPortrait.getOrElse("bedrooms", "0")
            rs.put(UserPortraitCommon.bedroomsTagCode, bedrooms)

            // 价格转换为价格段
            val price = inventoryPortrait.getOrElse("price", "0")
            val priceTierId = UserPortraitTags.getPriceTier(price)
            rs.put(UserPortraitCommon.priceTagCode, priceTierId)
        }

        rs
    }

    val sqlStmt = """
        SELECT
          city_id
          , district_id
          , block_id
          , community_id
          , id
          , price
          , area
          , publish_time AS created_at
          , orientation
          , bedrooms
          , floor
          , total_floors
        FROM angejia.marketing_inventory
        WHERE id = %d
        """
}
