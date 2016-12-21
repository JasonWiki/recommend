package com.angejia.dw.service.property;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.angejia.dw.common.util.DateUtil;
import com.angejia.dw.common.util.FileUtil;
import com.angejia.dw.common.util.mysql.JavaMysqlClient;
import com.angejia.dw.hadoop.hive.HiveClient;
import com.angejia.dw.service.Conf;

/**
 * 房源主题 Build 数据
 * 
 * @author Jason
 */
public class PropertyInventoryService {

    // Hive
    private HiveClient sparkHiveClient;

    // 业务 Mysql
    private JavaMysqlClient productJavaMysqlClient;

    // 业务 Mysql
    private JavaMysqlClient dwJavaMysqlClient;

    // date file 日期映射
    private String datePointFile;
    private String datePoint;
    private String curDate;

    /**
     * 初始化
     * 
     * @param env
     *            "dev" 表示测试环境, "online" 表示线上环境
     */
    public PropertyInventoryService(String env, String datePointFile) {
        /**
         * 配置文件
         */
        Conf conf = new Conf();
        conf.setEnv(env);

        /**
         * 初始化 业务 mysql 连接
         */
        String productMysqHost = conf.getProductMysqDBInfo().get("host");
        String productMysqAccount = conf.getProductMysqDBInfo().get("account");
        String productMysqPassword = conf.getProductMysqDBInfo().get("password");
        String productMysqDefaultDB = conf.getProductMysqDBInfo().get("defaultDB");
        // 连接
        this.productJavaMysqlClient = new JavaMysqlClient(
                "jdbc:mysql://" + productMysqHost + ":3306/" + productMysqDefaultDB
                        + "?useUnicode=true&characterEncoding=utf-8",
                productMysqAccount,
                productMysqPassword);

        /**
         * 初始化 dw mysql 连接
         */
        String dwMysqHost = conf.getDwMysqDBInfo().get("host");
        String dwMysqAccount = conf.getDwMysqDBInfo().get("account");
        String dwMysqPassword = conf.getDwMysqDBInfo().get("password");
        String dwMysqDefaultDB = conf.getDwMysqDBInfo().get("defaultDB");
        // 连接
        this.dwJavaMysqlClient = new JavaMysqlClient(
                "jdbc:mysql://" + dwMysqHost + ":3306/" + dwMysqDefaultDB + "?useUnicode=true&characterEncoding=utf-8",
                dwMysqAccount,
                dwMysqPassword);

        /**
         * 初始化 sparkHive 连接
         */
        String sparkThriftServerUrl = conf.getSparkConf().get("sparkThriftServerUrl");
        String sparkThriftServerUser = conf.getSparkConf().get("sparkThriftServerUser");
        String sparkThriftServerPass = conf.getSparkConf().get("sparkThriftServerPass");
        try {
            this.sparkHiveClient = new HiveClient(sparkThriftServerUrl, sparkThriftServerUser, sparkThriftServerPass);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 读取日期文件中的日期
        this.datePointFile = datePointFile;
        this.datePoint = FileUtil.fileInputStream(this.datePointFile);
        this.curDate = DateUtil.getCurTime(DateUtil.SIMPLE_FORMAT);
    }

    /**
     * build 安个家房源
     * 
     * @param buildNum
     *            每次处理条数
     * @throws SQLException
     */
    private void buildInventoryRun(int buildNum) {
        // 获取记录条数
        int mysqlSourceCount = 0;
        String countInventorySql = countInventorySqltmpl.replace("${date}", datePoint);

        // 获取记录条数
        mysqlSourceCount = productJavaMysqlClient.count(countInventorySql);

        // 分页次数
        int pageNum = mysqlSourceCount / buildNum;

        // 每次批量写入数据
        for (int i = 0; i <= pageNum; i++) {
            // 开始数
            int offset = i * buildNum;
            this.buildInventory(datePoint, offset, buildNum);
        }
    }

    /**
     * build 数据实体
     * 
     * @param date
     *            日期 "2016-01-01 00:00:00"
     * @param offset
     *            查询条数位置
     * @param limit
     *            每次查询数据量
     * @throws SQLException
     */
    private void buildInventory(String date, Integer offset, Integer limit) {
        String readInventorySql = readInventorySqltmpl.replace("${date}", date)
                .replace("${offset}", offset.toString())
                .replace("${limit}", limit.toString());
        // 查询用到的字段
        String mysqlSourceFields = "id,city_id,district_id,block_id,community_id,bedrooms,price,price_tier,area,"
                + "status,is_real,survey_status,provider_id,"
                + "created_at,updated_at";
        System.out.println(readInventorySql);

        // mysql 最新更新房源数据
        List<Map<String, String>> mysqlSourceData = productJavaMysqlClient.select(readInventorySql, mysqlSourceFields);

        // mysql 房源 ids
        List<String> inventoryIdsArr = new ArrayList<String>();
        for (int i = 0; i < mysqlSourceData.size(); i++) {
            Map<String, String> row = mysqlSourceData.get(i);
            // 房源 id
            String inventoryId = row.get("id");
            inventoryIdsArr.add(inventoryId);
        }

        if (inventoryIdsArr.size() == 0)
            return;

        String inventoryIdsStr = StringUtils.join(inventoryIdsArr, ",");

        // Hive 房源数据数据, 通过 房源 ids 获取数据
        Map<String, Map<String, String>> hiveSourceDataFomatKey = new HashMap<String, Map<String, String>>();
        try {
            // sql
            String readHiveInventorySql = readHiveInventorySqltmpl.replace("${ids}", inventoryIdsStr);
            // 字段
            String hiveSourceFields = "inventory_id,inventory_type,inventory_type_id";
            System.out.println(readHiveInventorySql);

            // 查询
            List<Map<String, String>> hiveSourceData = this.sparkHiveClient.select(readHiveInventorySql,
                    hiveSourceFields);

            // 按照 inventoryId 为 key 重新组织数据
            for (int i = 0; i <= hiveSourceData.size() - 1; i++) {
                Map<String, String> row = hiveSourceData.get(i);

                String inventoryId = row.get("inventory_id");
                hiveSourceDataFomatKey.put(inventoryId, row);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        StringBuilder executeSql = new StringBuilder("REPLACE INTO "
                + "dw_service.proerty_inventory_index("
                + "inventory_id,city_id,district_id,block_id,community_id,bedrooms,price,"
                + "price_tier,inventory_type,inventory_type_id,is_real,status,survey_status,"
                + "updated_at,is_marketing,provider_id"
                + ") VALUES ");
        // build 数据
        for (int i = 0; i < mysqlSourceData.size(); i++) {

            // mysql 源中的数据
            Map<String, String> row = mysqlSourceData.get(i);
            String inventoryId = row.get("id");
            String cityId = row.get("city_id");
            String districtId = row.get("district_id");
            String blockId = row.get("block_id");
            String communityId = row.get("community_id");
            // 户型
            String bedrooms = row.get("bedrooms");
            // 房源价格
            String price = row.get("price");
            // 价格段
            String priceTier = row.get("price_tier");

            // 在售: 2 在卖
            String status = row.get("status");
            // 实堪: 2 已实勘
            String surveyStatus = row.get("survey_status");
            String isReal = row.get("is_real");
            String providerId = row.get("provider_id");

            // 修改时间
            String updatedAt = row.get("updated_at");

            // hive 中的数据
            // 房源类型
            Map<String, String> hiveRow = hiveSourceDataFomatKey.get(inventoryId);
            // 房源类型分层
            String inventoryType = "0";
            String inventoryTypeId = "0";
            if (hiveRow != null) {
                inventoryType = hiveRow.get("inventory_type");
                inventoryTypeId = hiveRow.get("inventory_type_id");
                if (inventoryType == null || inventoryType == "null") {
                    inventoryType = "0";
                }
                if (inventoryTypeId == null || inventoryTypeId == "null") {
                    inventoryTypeId = "0";
                }
            }

            if (i != 0)
                executeSql.append(",");

            executeSql.append("("
                    + "'" + inventoryId + "',"
                    + "'" + cityId + "',"
                    + "'" + districtId + "',"
                    + "'" + blockId + "',"
                    + "'" + communityId + "',"
                    + "'" + bedrooms + "',"
                    + "'" + price + "',"
                    + "'" + priceTier + "',"
                    + "'" + inventoryType + "',"
                    + "'" + inventoryTypeId + "',"
                    + "'" + isReal + "',"
                    + "'" + status + "',"
                    + "'" + surveyStatus + "',"
                    + "'" + updatedAt + "',"
                    + "0,"
                    + providerId
                    + ")");
        }

        System.out.println(executeSql);
        this.dwJavaMysqlClient.execute(executeSql.toString());
        System.out.println("本次写入: " + mysqlSourceData.size());
    }

    /**
     * build 营销房源
     * 
     * @param buildNum
     *            每次处理条数
     * @throws SQLException
     */
    private void buildMarketingInventoryRun(int buildNum) {
        // 获取记录条数
        int mysqlSourceCount = 0;
        String countMarketingInventorySql = countMarketingInventorySqltmpl.replace("${date}", datePoint);

        // 获取记录条数
        mysqlSourceCount = productJavaMysqlClient.count(countMarketingInventorySql);

        // 分页次数
        int pageNum = mysqlSourceCount / buildNum;

        // 每次批量写入数据
        for (int i = 0; i <= pageNum; i++) {
            // 开始数
            int offset = i * buildNum;
            this.buildMarketingInventory(datePoint, offset, buildNum);
        }
    }

    /**
     * build 数据实体
     * 
     * @param date
     *            日期 "2016-01-01 00:00:00"
     * @param offset
     *            查询条数位置
     * @param limit
     *            每次查询数据量
     * @throws SQLException
     */
    private void buildMarketingInventory(String date, Integer offset, Integer limit) {
        String readMarketingInventorySql = readMarketingInventorySqltmpl.replace("${date}", date)
                .replace("${offset}", offset.toString())
                .replace("${limit}", limit.toString());
        // 查询用到的字段
        String mysqlSourceFields = "id,city_id,district_id,block_id,community_id,bedrooms,price,price_tier,area,"
                + "status,is_real,survey_status,provider_id,"
                + "created_at,updated_at";
        System.out.println(readMarketingInventorySql);

        // mysql 最新更新房源数据
        List<Map<String, String>> mysqlSourceData = productJavaMysqlClient.select(readMarketingInventorySql,
                mysqlSourceFields);

        // mysql 房源 ids
        List<String> inventoryIdsArr = new ArrayList<String>();
        for (int i = 0; i < mysqlSourceData.size(); i++) {
            Map<String, String> row = mysqlSourceData.get(i);
            // 房源 id
            String inventoryId = row.get("id");
            inventoryIdsArr.add(inventoryId);
        }

        StringBuilder executeSql = new StringBuilder("REPLACE INTO "
                + "dw_service.proerty_inventory_index("
                + "inventory_id,city_id,district_id,block_id,community_id,bedrooms,price,"
                + "price_tier,inventory_type,inventory_type_id,is_real,status,survey_status,"
                + "updated_at,is_marketing,provider_id"
                + ") VALUES ");
        // build 数据
        for (int i = 0; i < mysqlSourceData.size(); i++) {
            // mysql 源中的数据
            Map<String, String> row = mysqlSourceData.get(i);
            String inventoryId = row.get("id");
            String cityId = row.get("city_id");
            String districtId = row.get("district_id");
            String blockId = row.get("block_id");
            String communityId = row.get("community_id");
            // 户型
            String bedrooms = row.get("bedrooms");
            // 房源价格
            String price = row.get("price");
            // 价格段
            String priceTier = row.get("price_tier");

            // 在售: 2 在卖
            String status = row.get("status");
            // 实堪: 2 已实勘
            String surveyStatus = row.get("survey_status");
            String isReal = row.get("is_real");
            String providerId = row.get("provider_id");

            // 创建时间, 修改时间
            String updatedAt = row.get("updated_at");

            // 房源类型分层
            String inventoryType = "0";
            String inventoryTypeId = "" + (int) (Math.random() * 5);

            if (i != 0)
                executeSql.append(",");

            executeSql.append("("
                    + "'" + inventoryId + "',"
                    + "'" + cityId + "',"
                    + "'" + districtId + "',"
                    + "'" + blockId + "',"
                    + "'" + communityId + "',"
                    + "'" + bedrooms + "',"
                    + "'" + price + "',"
                    + "'" + priceTier + "',"
                    + "'" + inventoryType + "',"
                    + "'" + inventoryTypeId + "',"
                    + "'" + isReal + "',"
                    + "'" + status + "',"
                    + "'" + surveyStatus + "',"
                    + "'" + updatedAt + "',"
                    + "1,"
                    + providerId
                    + ")");
        }

        System.out.println(executeSql);
        this.dwJavaMysqlClient.execute(executeSql.toString());
        System.out.println("本次写入: " + mysqlSourceData.size());
    }

    public static void main(String[] args) {
        for (String a : args) {
            System.out.println(a);
        }
        String env = args[0];
        String datePointFile = args[1];

        /**
         * 执行抽取数据到 MySQL
         */
        PropertyInventoryService propertyInventory = new PropertyInventoryService(env, datePointFile);

        propertyInventory.buildInventoryRun(1000);
        propertyInventory.buildMarketingInventoryRun(1000);

        propertyInventory.saveDatePoint();
    }

    public void saveDatePoint() {
        // 更新到文件日期到当前时间
        System.out.println("saveDatePoint: " + curDate);
        FileUtil.fileOutputStream(this.datePointFile, curDate, false);
    }

    public void dump(Object ob) {
        System.out.println(ob);
        System.exit(0);
    }

    private static String countInventorySqltmpl = "SELECT COUNT(*) AS cn FROM property.inventory WHERE updated_at > '${date}'";
    private static String readInventorySqltmpl = ""
            + "SELECT"
            + "  inventory.id AS id"
            + "  ,inventory.city_id AS city_id"
            + "  ,community.district_id AS district_id"
            + "  ,community.block_id AS block_id"
            + "  ,house.community_id AS community_id"
            + "  ,property.bedrooms AS bedrooms"
            + "  ,inventory.price AS price"
            + "  ,CASE"
            + "    WHEN inventory.price >= 0 AND inventory.price <= 1500000 THEN 1"
            + "    WHEN inventory.price >= 1500000 AND inventory.price <= 2000000 THEN 2"
            + "    WHEN inventory.price >= 2000000 AND inventory.price <= 2500000 THEN 3"
            + "    WHEN inventory.price >= 2500000 AND inventory.price <= 3000000 THEN 4"
            + "    WHEN inventory.price >= 3000000 AND inventory.price <= 4000000 THEN 5"
            + "    WHEN inventory.price >= 4000000 AND inventory.price <= 5000000 THEN 6"
            + "    WHEN inventory.price >= 5000000 AND inventory.price <= 7000000 THEN 7"
            + "    WHEN inventory.price >= 7000000 AND inventory.price <= 10000000 THEN 8"
            + "    WHEN inventory.price >= 10000000 AND inventory.price <= 9999999999 THEN 9"
            + "  END AS price_tier"
            + "  ,inventory.area AS area"
            + "  ,inventory.is_real AS is_real"
            + "  ,inventory.survey_status AS survey_status"
            + "  ,inventory.verify_status AS verify_status"
            + "  ,inventory.status AS status"
            + "  ,inventory.provider_id AS provider_id"
            + "  ,inventory.created_at AS created_at"
            + "  ,inventory.updated_at AS updated_at "
            + "FROM property.inventory AS inventory "
            + "LEFT JOIN property.property AS property on inventory.property_id = property.id "
            + "LEFT JOIN property.house AS house on property.house_id = house.id "
            + "LEFT JOIN angejia.community AS community on house.community_id = community.id "
            + "WHERE (inventory.updated_at <> '0000-00-00 00:00:00' AND inventory.created_at <> '0000-00-00 00:00:00') "
            + "AND inventory.updated_at > '${date}' "
            + "ORDER BY inventory.id "
            + "LIMIT ${offset},${limit}";
    private static String readHiveInventorySqltmpl = ""
            + "SELECT inventory_id, inventory_type,"
            + "CASE "
            // -- A 类, B 类, C 类房源 排名
            + "  WHEN inventory_type = 'A' THEN 1 "
            + "  WHEN inventory_type = 'B' THEN 2 "
            + "  WHEN inventory_type = 'C' THEN 3 "
            + "  WHEN inventory_type = 'D' THEN 4 "
            + "  ELSE 5 "
            + "END AS inventory_type_id "
            + "FROM dw_db.dw_property_inventory_level"
            + "  WHERE inventory_id IN (${ids})";

    private static String countMarketingInventorySqltmpl = "SELECT COUNT(*) AS cn FROM angejia.marketing_inventory WHERE updated_at > '${date}'";
    private static String readMarketingInventorySqltmpl = ""
            + "SELECT "
            + "  id,city_id,district_id,block_id,community_id,bedrooms,price,area "
            + "  ,CASE WHEN price >= 0 AND price <= 1500000 THEN 1 "
            + "    WHEN price >= 1500000 AND price <= 2000000 THEN 2 "
            + "    WHEN price >= 2000000 AND price <= 2500000 THEN 3 "
            + "    WHEN price >= 2500000 AND price <= 3000000 THEN 4 "
            + "    WHEN price >= 3000000 AND price <= 4000000 THEN 5 "
            + "    WHEN price >= 4000000 AND price <= 5000000 THEN 6 "
            + "    WHEN price >= 5000000 AND price <= 7000000 THEN 7 "
            + "    WHEN price >= 7000000 AND price <= 10000000 THEN 8 "
            + "    WHEN price >= 10000000 AND price <= 9999999999 THEN 9 "
            + "  END AS price_tier "
            + "  ,1 AS is_real,2 AS survey_status,audit_status AS verify_status,2 AS `status` "
            + "  ,provider_id AS provider_id"
            + "  ,created_at,updated_at "
            + "FROM angejia.marketing_inventory "
            + "WHERE updated_at != '0000-00-00 00:00:00' AND created_at != '0000-00-00 00:00:00' "
            + "  AND updated_at > '${date}' "
            + "ORDER BY id "
            + "LIMIT ${offset},${limit} ";
}
