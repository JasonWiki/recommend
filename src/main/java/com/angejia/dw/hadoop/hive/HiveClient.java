package com.angejia.dw.hadoop.hive;


import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;


import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hive.jdbc.HiveDriver;


public class HiveClient {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * 获取连接
     */
    private Connection connection;
    public Connection getConnection() {
        return connection;
    }
    public void setConnection(Connection conn) {
        this.connection = conn;
    }



    public HiveClient(String url, String user, String password) throws SQLException {
        // 导入类
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        // 创建连接
        Connection con = DriverManager.getConnection(url, user, password);
        this.setConnection(con);

        // 创建连接句柄语句
        //Statement stmt = con.createStatement();
        //this.setStmt(stmt);
    }


    /**
     * 执行指定 Sql
     * @param sql
     * @return 布尔值
     */
    public Boolean execute (String sql) {
        Boolean rs = false;
        try {
            Statement stmt = this.getConnection().createStatement();
            rs = stmt.execute(sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }


    /**
     * 查询数据 
     * @param sql
     * @param fields 字段 
     * @return List<Map<String, String>>
     * @throws SQLException
     * 
     * 循环出数据
     * for (i <- 0 to select.size() - 1 ) {
            println(rsData.get(i).get("visit_item_invs_a"))
        }
     * 
     */
    public List<Map<String, String>> select(String sql, String fields) throws SQLException {
        // 保存结果数据
        List<Map<String, String>> listResult = new ArrayList<Map<String, String>>();

        ResultSet res = null;
        try {
            Statement stmt = this.getConnection().createStatement();

            res = stmt.executeQuery(sql);

            // 转换为数组
            String[] arrFields = fields.split(",");

            // 遍历每一行
            while (res.next()) {
                // 保存一行数据
                Map<String, String> mapRowData = new HashMap<String, String>();

                // 拼接字段值
                for (String field : arrFields) {
                    mapRowData.put(field, res.getString(field));
                }
                // 追加到 list 中
                listResult.add(mapRowData);

                mapRowData = null;
            }

            stmt.close();
            res.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //res.close();
        /**
        for (Map<String, String> rs : listResult) {
            System.out.println(rs.get("broker_id"));
        }
        */

        return listResult;
    }


    /**
     * 关闭连接
     */
    public void closeConnection() {
        try {
            this.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }










    /**
     * 测试方法
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
       try {
        Class.forName(driverName);
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        System.exit(1);
      }
      //replace "hive" here with the name of the user the queries should run as
      Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
      Statement stmt = con.createStatement();
      String tableName = "testHiveDriverTable";
      stmt.execute("drop table if exists " + tableName);
      stmt.execute("create table " + tableName + " (key int, value string)");
      // show tables
      String sql = "show tables '" + tableName + "'";
      System.out.println("Running: " + sql);
      ResultSet res = stmt.executeQuery(sql);
      if (res.next()) {
        System.out.println(res.getString(1));
      }
         // describe table
      sql = "describe " + tableName;
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      while (res.next()) {
        System.out.println(res.getString(1) + "\t" + res.getString(2));
      }
   
      // load data into table
      // NOTE: filepath has to be local to the hive server
      // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
      String filepath = "/tmp/a.txt";
      sql = "load data local inpath '" + filepath + "' into table " + tableName;
      System.out.println("Running: " + sql);
      stmt.execute(sql);
   
      // select * query
      sql = "select * from " + tableName;
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      while (res.next()) {
        System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
      }
   
      // regular hive query
      sql = "select count(1) from " + tableName;
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      while (res.next()) {
        System.out.println(res.getString(1));
      }
    }
}
