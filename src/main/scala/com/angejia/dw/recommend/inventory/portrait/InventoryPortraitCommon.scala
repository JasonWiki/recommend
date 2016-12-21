package com.angejia.dw.recommend.inventory.portrait

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

import com.angejia.dw.common.util.mysql.MysqlClient
import com.angejia.dw.recommend.user.portrait.UserPortraitCommon
import com.angejia.dw.recommend.user.portrait.UserPortraitTags

object InventoryPortraitCommon {

    // mysql 数据库连接对象
    var mysqlClient: MysqlClient = null

    /**
     * 通过房源 Id 获取房源画像基础数据
     */
    private def getInventoryPortraitByInventoryId(inventoryId: String): HashMap[String, String] = {
        var querySql = sqlStmt.format(inventoryId.toInt)
        val res = mysqlClient.select(querySql)
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
          community.city_id AS city_id
          ,community.district_id AS district_id
          ,community.block_id AS block_id
          ,house.community_id AS community_id
          ,inventory.id AS inventory_id
          ,inventory.price AS price
          ,inventory.area AS area
          ,inventory.is_real AS is_real
          ,inventory.survey_status AS survey_status
          ,inventory.source AS source
          ,inventory.has_checked AS has_checked
          ,inventory.created_at AS created_at
          ,inventory.updated_at AS updated_at
          ,inventory.verify_status AS verify_status
          ,inventory.status AS status
          ,house.orientation AS orientation
          ,property.bedrooms AS bedrooms
          ,house.floor AS floor
          ,house.total_floors AS total_floors
        FROM
          property.inventory AS inventory
          LEFT JOIN property.property AS property on inventory.property_id = property.id
          LEFT JOIN property.house AS house on property.house_id = house.id
          LEFT JOIN angejia.community AS community on house.community_id = community.id
        WHERE inventory.id = %d
        """
}
