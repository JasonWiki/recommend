package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import sun.misc.BASE64Decoder

import com.angejia.dw.hadoop.hbase.HBaseClient
import com.angejia.dw.common.util.mysql.MysqlClient
import com.angejia.dw.hadoop.hive.HiveClient

import com.angejia.dw.common.util.JsonUtil

object UserPortraitCommon {

    // mysql 数据库连接对象
    var mysqlClient: MysqlClient = null

    // spark 通过 thriftServer 连接 hive 数据仓库的对象
    var sparkHiveClient: HiveClient = null

    // 用户画像表连接对象
    var userPortraitTable: HBaseClient = null

    // 用户画像标签 列族
    val TagColumnFamily = "tags"

    // 用户画像维度 列族
    val DimColumnFamily = "dimension"

    // 用户画像需求 列族
    val NeedsColumnFamily = "needs"

    // 用户画像推荐 列族(保存推荐信息)
    val ModelStateColumnFamily = "modelState"

    // 标签配置！ 
    // 城市
    val cityTagConf = UserPortraitTagConf.CITY_TAG // 标签配置
    val cityTagCode = cityTagConf.getOrElse("TagCode", "") // 标签代码

    // 区域
    val districtTagConf = UserPortraitTagConf.DISTRICT_TAG
    val districtTagCode = districtTagConf.getOrElse("TagCode", "")

    // 版块
    val blockTagConf = UserPortraitTagConf.BLOCK_TAG
    val blockTagCode = blockTagConf.getOrElse("TagCode", "")

    // 小区
    val communityTagConf = UserPortraitTagConf.COMMUNITY_TAG
    val communityTagCode = communityTagConf.getOrElse("TagCode", "")

    // 户型
    val bedroomsTagConf = UserPortraitTagConf.BEDROOMS_TAG
    val bedroomsTagCode = bedroomsTagConf.getOrElse("TagCode", "").toString()
    // 户型段映射
    val bedroomsType = bedroomsTagConf.get("bedroomsType").get.asInstanceOf[collection.Map[String, String]]

    // 价格
    val priceTagConf = UserPortraitTagConf.PRICE_TAG
    val priceTagCode = priceTagConf.getOrElse("TagCode", "").toString()
    // 价格段
    val priceTier = priceTagConf.get("PriceTier").get.asInstanceOf[collection.Map[String, String]]
    // 价格段映射
    val priceTierType = priceTagConf.get("PriceTierType").get.asInstanceOf[collection.Map[String, String]]

    /**
     * 获取用户画像 tags 标签列族数据
     * userId 用户 ID
     * columnFamily 列族
     */
    def getUserPortraitTagsByUserId(userId: String): HashMap[String, String] = {

        val hbaseData: HashMap[String, String] = UserPortraitCommon.userPortraitTable.select(
            userId,
            UserPortraitCommon.TagColumnFamily,
            Array(UserPortraitCommon.cityTagCode,
                UserPortraitCommon.districtTagCode,
                UserPortraitCommon.blockTagCode,
                UserPortraitCommon.communityTagCode,
                UserPortraitCommon.bedroomsTagCode,
                UserPortraitCommon.priceTagCode))

        hbaseData
    }

    /**
     * 获取用户画像 dim 维度列族数据
     * userId 用户 ID
     * columnFamily 列族
     */
    def getUserPortraitDimByUserId(userId: String): HashMap[String, String] = {

        val hbaseData: HashMap[String, String] = UserPortraitCommon.userPortraitTable.select(
            userId,
            UserPortraitCommon.DimColumnFamily,
            Array( //"userDemand",        // 需求单维度老的,
                "memberDemand", // 需求单维度
                "likeInventorys", // 收藏房源
                "visitItemInventorys", // 带看房源
                "linkInventorys" // 连接房源
                ))

        hbaseData
    }

    /**
     * 获取用户画像 needs 列族需求数据
     * userId 用户 ID
     * columnFamily 列族
     */
    def getUserPortraitNeedsByUserId(userId: String): HashMap[String, String] = {

        val hbaseData: HashMap[String, String] = UserPortraitCommon.userPortraitTable.select(
            userId,
            UserPortraitCommon.NeedsColumnFamily, // 列族
            Array("actionNeeds" // 需要抽取的列
            ))

        hbaseData
    }

    /**
     * 获取用户画像 ModelState 建模状态列族数据
     */
    def getUserPortraitModelStateByUserId(userId: String): HashMap[String, String] = {

        val hbaseData: HashMap[String, String] = UserPortraitCommon.userPortraitTable.select(
            userId,
            UserPortraitCommon.ModelStateColumnFamily, // 列族
            // 需要抽取的列
            Array("visitItemInventorysRecord", // 带看上次修改记录
                "linkInventorysRecord", // 连接上次修改记录
                "memberDemandTime" // 需求单上次修改记录
                ))

        hbaseData
    }

    /**
     * 对于 map 中,key 是空的值，进行处理
     */
    def mapKeyDefaultValue(map: HashMap[String, String], key: String, default: String = ""): String = {
        var rs: String = ""
        if (map.contains(key)) {
            if (map.getOrElse(key, null) == null) {
                rs = default
            } else {
                rs = map.get(key).get.toString()
            }
        } else {
            rs = default
        }
        rs
    }

    /**
     * 解密 auth
     */
    def Decrypt(data: String): String = {
        try {
            val key = "12345678123456xx"
            val iv = "12345678123456xx"

            val encrypted1 = new BASE64Decoder().decodeBuffer(data)

            val cipher = Cipher.getInstance("AES/CBC/NoPadding");
            val keyspec = new SecretKeySpec(key.getBytes(), "AES");
            val ivspec = new IvParameterSpec(iv.getBytes());

            cipher.init(Cipher.DECRYPT_MODE, keyspec, ivspec);
            val original = cipher.doFinal(encrypted1)
            val originalString = new String(original)
            return originalString;

        } catch {
            case e: Exception =>
                e.printStackTrace();
                return "";
        }
    }

    /**
     * Json 转换成 Map, 不可变的 Map
     */
    def jsonStrToMap(jsonString: String): Map[String, Object] = {
        JsonUtil.smartJsonStrToMap(jsonString)
    }

    /**
     * Map 转换成 String, 不可变的 Map
     */
    def mapToJsonStr(map: Map[String, Object]): String = {
        JsonUtil.smartMapToJsonStr(map)
    }

    /**
     * 两层 json 转换 -> 可变的 Map
     * jsonStr :
     *  {}
     *  或者
     *  {
     *    "x": {"a":"1","b":"2"},
     *    "y": {"c":"3","d":"4"}
     *   }
     *
     * return :
     *  Map(
     *   "x" => Map("a"=> 1, "b"=> 2),
     *   "y" => Map("c"=> 3, "d"=> 4)
     *   )
     */
    def jsonStrToMapByTwolayers(jsonStr: String): Map[String, Map[String, Object]] = {
        // 返回的是一个 Map[String, Object] , Object = Map[String, String]
        val baseMap = JsonUtil.playJsonToMap(jsonStr)
        // json 转换为可变 map 
        val mapChildToVariable = baseMap.map {
            case (k, v) =>
                val curK = k
                // 把 v 转换为 Map[String,String]
                val curV = v.asInstanceOf[scala.collection.immutable.Map[String, Object]]
                // 再把 map 转换为可变 Map
                val formatV = scala.collection.mutable.Map(curV.toSeq: _*)
                k -> formatV
        }
        // 转变 map 数据
        val mapVariable = collection.mutable.Map(mapChildToVariable.toSeq: _*).asInstanceOf[scala.collection.mutable.Map[String, Map[String, Object]]]
        mapVariable
    }

    /**
     * 两层 map 转换 -> 为 json
     */
    def mapToJsonStrByTwolayers(mapData: Map[String, Map[String, Object]]): String = {
        val mapDataToMap = mapData.map {
            case (k, v) => k -> v.toMap // 转换成不可变 Map
        }.toMap
        // 转换为 json 字符串
        val mapToStr = JsonUtil.playMapToJson(mapDataToMap)
        mapToStr
    }
}
