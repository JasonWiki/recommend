package com.angejia.dw.recommend.user.portrait

import scala.collection.mutable.Map

/**
 * 用户画像 标签核心配置
 */

object UserPortraitTagConf {

    // 城市标签
    val CITY_TAG = Map(
        "TagCode" -> "city",
        "TagName" -> "城市",

        // 用户选房单分数
        "userDemandScore" -> "0",

        // 房源筛选给的分数
        "filterScore" -> "0",

        // 浏览房源分数
        "browseScore" -> "0",

        // 收藏房源分数
        "likeInventoryScore" -> "0",

        // 发生带看房源发生的分数
        "visitItemInventoryScore" -> "0",

        // 发生连接的分数
        "linkInventoryScore" -> "0",

        // 分数衰减百分比
        "attenuationPercentage" -> "0.1")

    // 区域标签
    val DISTRICT_TAG = Map(
        "TagCode" -> "district",
        "TagName" -> "区域",

        // 用户选房单分数
        "userDemandScore" -> "0",

        // 房源筛选给的分数
        "filterScore" -> "0",

        // 浏览房源分数
        "browseScore" -> "0",

        // 收藏房源分数
        "likeInventoryScore" -> "0",

        // 发生带看房源发生的分数
        "visitItemInventoryScore" -> "0",

        // 发生连接的分数
        "linkInventoryScore" -> "0",

        // 分数衰减百分比
        "attenuationPercentage" -> "0.1")

    // 版块标签
    val BLOCK_TAG = Map(
        "TagCode" -> "block",
        "TagName" -> "版块",

        // 用户选房单分数
        "userDemandScore" -> "20",

        // 房源筛选给的分数
        //"filterScore" -> "2",
        "filterScore" -> "10",

        // 浏览房源分数
        "browseScore" -> "1",

        // 收藏房源分数
        "likeInventoryScore" -> "5",

        // 发生带看房源发生的分数
        "visitItemInventoryScore" -> "50",

        // 发生连接的分数
        "linkInventoryScore" -> "30",

        // 分数衰减百分比
        "attenuationPercentage" -> "0.1")

    // 小区标签
    val COMMUNITY_TAG = Map(
        "TagCode" -> "community",
        "TagName" -> "小区",

        // 用户选房单分数
        "userDemandScore" -> "20",

        // 房源筛选给的分数
        //"filterScore" -> "10",
        "filterScore" -> "10", // 1.0

        // 浏览房源分数
        //"browseScore" -> "2",
        "browseScore" -> "5", // 1.0

        // 收藏房源分数
        "likeInventoryScore" -> "5",

        // 发生带看房源发生的分数
        "visitItemInventoryScore" -> "50",

        // 发生连接的分数
        "linkInventoryScore" -> "30",

        // 分数衰减百分比
        "attenuationPercentage" -> "0.1")

    // 户型标签 
    val BEDROOMS_TAG = Map(
        "TagCode" -> "bedrooms",
        "TagName" -> "户型",

        // 用户选房单分数
        "userDemandScore" -> "20",

        // 房源筛选给的分数
        //"filterScore" -> "2",
        "filterScore" -> "10", // 1.0 

        // 浏览房源分数
        //"browseScore" -> "2",
        "browseScore" -> "5", // 1.0

        // 收藏房源分数
        "likeInventoryScore" -> "5",

        // 发生带看房源发生的分数
        "visitItemInventoryScore" -> "50",

        // 发生连接的分数
        "linkInventoryScore" -> "30",

        // 分数衰减百分比
        "attenuationPercentage" -> "0.1",

        // 户型映射(筛选列表时)
        "bedroomsType" -> Map[String, String](
            "2" -> "1",
            "3" -> "2",
            "4" -> "3",
            "5" -> "4",
            "6" -> "5",
            "7" -> "6"))

    // 价格段标签
    val PRICE_TAG = Map(
        "TagCode" -> "price",
        "TagName" -> "价格段",

        // 用户选房单分数
        "userDemandScore" -> "20",

        // 房源筛选给的分数
        //"filterScore" -> "2",
        "filterScore" -> "10", // 1.0

        // 浏览房源分数
        //"browseScore" -> "2",
        "browseScore" -> "5", // 1.0

        // 收藏房源分数
        "likeInventoryScore" -> "5",

        // 发生带看房源发生的分数
        "visitItemInventoryScore" -> "50",

        // 发生连接的分数
        "linkInventoryScore" -> "30",

        // 分数衰减百分比
        "attenuationPercentage" -> "0.1",

        // 价格段映射
        "PriceTierType" -> Map[String, String](
            "2" -> "1",
            "3" -> "2",
            "4" -> "3",
            "5" -> "4",
            "6" -> "5",
            "7" -> "6",
            "8" -> "7",
            "9" -> "8",
            "10" -> "9"),

        // 价格段数据
        "PriceTier" -> Map[String, String](
            "0-1500000" -> "1",
            "1500000-2000000" -> "2",
            "2000000-2500000" -> "3",
            "2500000-3000000" -> "4",
            "3000000-4000000" -> "5",
            "4000000-5000000" -> "6",
            "5000000-7000000" -> "7",
            "7000000-10000000" -> "8",
            "10000000-1000000000" -> "9"))
}