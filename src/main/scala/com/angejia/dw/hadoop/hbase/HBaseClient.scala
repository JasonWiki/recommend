package com.angejia.dw.hadoop.hbase

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

// Hbase 基础类
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.HBaseAdmin


import org.apache.hadoop.hbase.client.Scan

/**
 * scan 比较运算符号
 * CompareFilter.CompareOp
 *  EQUAL 相等
    GREATER 大于
    GREATER_OR_EQUAL 大于等于
    LESS 小于
    LESS_OR_EQUAL 小于等于
    NOT_EQUAL 不等于
 */
import org.apache.hadoop.hbase.filter.CompareFilter


/**
 * 比较器
 */
import org.apache.hadoop.hbase.filter.RegexStringComparator // 匹配正则表达式
import org.apache.hadoop.hbase.filter.SubstringComparator    // 匹配子字符串
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator    //匹配前缀字节数据
import org.apache.hadoop.hbase.filter.BinaryComparator      // 匹配完整字节数组


/**
 * 过滤器 
 */
import org.apache.hadoop.hbase.filter.FilterList    // 多个过滤器组合 
    //FilterList.Operator.MUST_PASS_ALL(必须通过所有) 
    //FilterList.Operator.MUST_PASS_ONE(必须通过一个)
import org.apache.hadoop.hbase.filter.RowFilter    // row key 过滤器
import org.apache.hadoop.hbase.filter .PrefixFilter    // row key 前缀过滤器
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter // 列值过滤器
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter  // 列过滤器
import org.apache.hadoop.hbase.filter.ValueFilter    // 值


class HBaseClient(tableName: String, zookeeperIds: String)  {
 
    // 属性
    //val conf = new HBaseConfiguration()
    //    conf.set("hbase.zookeeper.quorum", zookeeperIds);

    val conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zookeeperIds);

    // 连接
    val connection = ConnectionFactory.createConnection(conf);
    //val connection = HConnectionManager.createConnection(conf);

    // admin 连接
    val adminConnection = connection.getAdmin();

    // Table 连接
    val tableConnection = connection.getTable(TableName.valueOf(tableName))
    //val tableConnection = new HTable(conf, tableName)

    // list Table
    def listTable() : Array[HTableDescriptor] = {
        adminConnection.listTables()
    }


    /**
     * 根据 row key 获取数据(完全匹配): 单条数据查询
     * @return the row pointed to by rowKey
     */
    def findByKey(rowKey: String) : Result = {
        val get = new Get(Bytes.toBytes(rowKey))
        // hbase get 方法完成单条数据的查询
        val result = this.tableConnection.get(get)

        result
    }


    /**
     * 根据 row-key 获取指定列族下的列数据: 单条数据查询
     */
    def select(
            rowKey: String,     // row-key
            columnFamily: String,     // 列族
            columnNames: Array[String]    // 列
    ) : HashMap[String, String] = {

        // 保存结果
        var result = new HashMap[String, String];

        // 通过 rowkey 查找数据
        val rowData = this.findByKey(rowKey)

        if(rowData != null && !rowData.isEmpty){
            columnNames.foreach { curColumnName =>
                val columnNameValue = rowData.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(curColumnName))
                val columnNameValueToString = Bytes.toString(columnNameValue) 
                if (columnNameValueToString != null) result.put(curColumnName, columnNameValueToString )
            }
        } 

        result
    }


    /**
     * 插入数据
     * rowKey 写入的 row key
     * columnFamily  列族
     * columnName 列
     * columnValue 值
     */
    def insert(rowKey: String, columnFamily: String, columnName: String, columnValue: String): String = {

        // 构架 row-key
        val put = new Put(Bytes.toBytes(rowKey))
        
        // 写入数据  (列族,列,值)
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName), 
                Bytes.toBytes(columnValue)
                )
 
        this.tableConnection.put(put)

        //this.tableConnection.flushCommits
        //this.clone()

        rowKey
    }


    /**
     * 修改同插入
     */
    def update(rowKey: String, columnFamily: String, columnName: String, columnValue: String) : String = {
        this.insert(rowKey, columnFamily, columnName, columnValue)
    }


    /**
     * 删除 rowKey
     */
    def deleteByKey(rowKey: String): String =  {

        val delete = new Delete(Bytes.toBytes(rowKey))

        val result = this.tableConnection.delete(delete)

        // this.tableConnection.close

        rowKey
    }


    /**
     * 删除 指定 row-key -> 列族 -> 列 
     */
    def deleteColumn(rowkey:String, columnFamily:String, columnName:String) = {
        val deleteColumn = new Delete(Bytes.toBytes(rowkey));
        if (!columnFamily.isEmpty()) {
            deleteColumn.deleteColumns(
               Bytes.toBytes(columnFamily),        //不加family、col，则删除整行
               Bytes.toBytes(columnName)
            );
        }
        this.tableConnection.delete(deleteColumn);
    }


    /**
     * 获取基本 scan 属性
     */
    def scanBase(): Scan = {
        // scan 扫描构造对象
        val scan = new Scan()

        /**
         * 设置过滤器
         * import org.apache.hadoop.hbase.filter.FilterList
         */
        //scan.setFilter(filterList)

        /** 指定最大的版本个数
         *  scan.setMaxVersions() : 获取所有版本数据
         *  scan.setMaxVersions(n) : 返回指定版本的数据
         *  不调用 : 返回最新的版本
         */
        // scan.setMaxVersions()

        /**
         * 指定范围搜索
         * scan.setStartRow(x$1) : 默认从动表头开始扫描
         * scan.setStopRow(x$1) : 指定结束的行,不含此行
         */
        //scan.setStartRow(x$1)
        //scan.setStopRow(x$1)

        /**
         * 指定返回多少 cell 数目, 避免 rok-key 一行中有太多列数据, 导致 OOM 
         * scan.setBatch(batch)
         */
        //scan.setBatch(100)

        scan
    }


    /**
     * 通过 scan 获取 hbase 表数据
     */
    def getTableDataByScan(scan: Scan): ResultScanner = {

        /**
         * ResultScanner 返回包含该每条 Result 的容器
         * Result 代表一行 row-key 数据
         *
         *    // 遍历所偶 ResultScanner 中的数据
         *    val iteratorRs = scanner.iterator()    // 迭代器
         *    while(iteratorRs.hasNext()) {
         *        // 一行 row-key 的所有值
         *        val rowResult: Result =  iteratorRs.next()
         *
         *        rowResult.getRow // 返回 row-key
         *        rowResult.raw() // 返回 row-key 中所有列的 key-> value
         *        rowResult.getValue // 根据 Column 来获取 Cell 的值
         *    }
         *
         */
        val scanner: ResultScanner = this.tableConnection.getScanner(scan)

        scanner
    }


    /**
     * 扫描数据, 通过过滤器 list : 批量数据查询
     * 返回一个迭代器 
     */
    def scanByFilterList(filterList: FilterList) : ResultScanner = {

        val scan = this.scanBase()

        // 设置筛选器
        scan.setFilter(filterList)

        // 获取数据
        val scanner = this.getTableDataByScan(scan)

        scanner
    }


    /**
     *  转换 Scanner 成 HashMap[String, HashMap[String, String]] 格式的数据
     */
    def transformScanner(scanner: ResultScanner, take: Int = 500) : HashMap[String, HashMap[String, String]] = {
        // 保存结果
        var result = new HashMap[String, HashMap[String, String]]

        val iteratorRs = scanner.iterator()    // 迭代器

        var i = 1
        // 遍历每个 row key 
        while(iteratorRs.hasNext()) {
            if (i > take) return result
            i += 1

            // row key 的所有列
            val rowResult: Result =  iteratorRs.next()

            if (rowResult != null) {
                // 保存当前 row key 所有列的 map
                val columnMap = new HashMap[String, String]

                rowResult.raw().foreach { x => 
                    // row key
                    val rowKeyC = Bytes.toString(x.getRow()) 
                    // 列族
                    val columnFamily = Bytes.toString(x.getFamily)
                    // 列
                    val column = Bytes.toString(x.getQualifier)
                    // 值
                    val value = Bytes.toString(x.getValue)    
                   
                    //println(rowKeyC, columnFamily, column, value)
                    columnMap.put(columnFamily + ":" + column, value)
                }

                // 当前 row key
                val rowKey = Bytes.toString(rowResult.getRow)
                columnMap.put("rowKey", rowKey)

                result.put(rowKey, columnMap)
            }
        }

        result
    }


    /**
     * 解析 scan Map 后的数据, 只返回第一条数据
     */
    def scanToolFindFirstMap (mapData: HashMap[String, HashMap[String, String]]): HashMap[String, String] = {
        var rs = HashMap[String, String]() // 返回结果

        if (!mapData.isEmpty) {
            
            val keys = mapData.keySet.toArray
            val key = keys(0)

            rs = mapData.getOrElse(key, rs)
        }

       rs
    }


    /**
     * 通过 row-key 正则方式扫描数据
     * 返回结构
     * HashMap( 
     *    "1_10_100_10000_10201" -> Map("attr:total_floors" -> "6", "attr:is_real" -> "1") 
     * )
     */
    def scanByRowFilterRegex(rowKeyRegex: String) : HashMap[String, HashMap[String, String]] = {
        // 过滤器
        val filter = new RowFilter(
                 CompareFilter.CompareOp.EQUAL,
                 new RegexStringComparator(rowKeyRegex)
                 )
 
        // 完全匹配过滤器
        val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL) 
        filterList.addFilter(filter)

        // 通过过滤器搜索数据
        val scanner: ResultScanner =  this.scanByFilterList(filterList)

        // 转换成 HashMap
        val rs = this.transformScanner(scanner)

        rs
    }


    /**
     * 通过 rowkey 正则和 ColumnValue  定位数据
     * 
     * 案例:
         // 定义 row-key 正则
         val rowKeyRegex = "^1_7_68_19109_(.*)"

         // 组合列值查询条件
         val filterConditions = ListBuffer[(String, String, CompareFilter.CompareOp, String)]()

         // attr:bedrooms = 1
         filterConditions.append(("attr","bedrooms",CompareFilter.CompareOp.EQUAL,"1")) 

         // attr:price = 1
         filterConditions.append(("attr","price",CompareFilter.CompareOp.EQUAL,"1"))

         // 调用
         scanByRowFilterRegexAndColumnValue(rowKeyRegex, filterConditions)
     */
    def scanByRowFilterRegexAndColumnValue(
        // rowkey 正则
        rowKeyRegex: String,

        // 列值查询条件
        filterConditions: ListBuffer[
            (String,      // 列族
             String,      // 列
            CompareFilter.CompareOp,     // 匹配模式, 大于、小于等等,详情看本类 import 文档
            String)],     // 值

        // 通过模式, 默认所有必须通过
        pass :FilterList.Operator = FilterList.Operator.MUST_PASS_ALL,

        // 取多少条
        take: Int = 30     
    ) : HashMap[String, HashMap[String, String]] = {
        // 匹配过滤器
        val filterList = new FilterList(pass) 

        // rowkey 过滤器
        val rowKeyfilter = new RowFilter(
                CompareFilter.CompareOp.EQUAL,    // = 等于
                new RegexStringComparator(rowKeyRegex))    // 正则匹配
        filterList.addFilter(rowKeyfilter)
 
        // 列值过滤器,组合
        filterConditions.foreach{f => 
            val columnValueFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(f._1), // 列族
                    Bytes.toBytes(f._2), // 列名
                    f._3,                // 比较器
                    Bytes.toBytes(f._4)) // 列值
            filterList.addFilter(columnValueFilter)
        }

        // 过滤器搜索数据
        val scanner: ResultScanner =  this.scanByFilterList(filterList)

        // 转换成 HashMap
        val rs = this.transformScanner(scanner, take)

        rs
    }
    
    
    
    def close(): Unit = {
        connection.close()
    }

    
    
    
    
    
    


    



    
}