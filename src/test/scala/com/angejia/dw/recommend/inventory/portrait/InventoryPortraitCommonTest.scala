package com.angejia.dw.recommend.inventory.portrait;

import collection.mutable.Stack
import collection.mutable.HashMap
import org.scalatest._
import com.angejia.dw.common.util.mysql.MysqlClient
import com.angejia.dw.recommend.Conf

class InventoryPortraitCommonTest extends FlatSpec with Matchers {
    Conf.setEnv("dev")
    val productMysqDBInfo = Conf.getProductMysqDBInfo()
    InventoryPortraitCommon.mysqlClient = new MysqlClient(
        productMysqDBInfo.get("host").get,
        productMysqDBInfo.get("account").get,
        productMysqDBInfo.get("password").get,
        productMysqDBInfo.get("defaultDB").get)
    
    "getInventoryPortraitByInventoryId" should "works" in {
        val res = InventoryPortraitCommon.getUserTagsInventoryMappingByInventoryId("1")

        res should contain key ("price")
        res should contain key ("district")
        res should contain key ("city")
        res should contain key ("block")
        res should contain key ("bedrooms")
        res should contain key ("community")
    }
}
