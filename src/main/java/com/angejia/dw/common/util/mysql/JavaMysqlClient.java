package com.angejia.dw.common.util.mysql;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JavaMysqlClient {

    private String url;
    private String user;
    private String psw;

    private Connection conn;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public JavaMysqlClient(String url, String user, String psw) {
        this.url = url;
        this.user = user;
        this.psw = psw;
    }

    /**
     * 获取数据库的连接
     * 
     * @return conn
     */
    public Connection getConnection() {
        if (null == conn) {
            try {
                conn = DriverManager.getConnection(url, user, psw);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return conn;
    }

    /**
     * 查询数据
     * 
     * @param sql
     *            查询的 SQL
     * @param fields
     *            查询的字段 fields
     * @return List<Map<String, String>>
     * @throws SQLException
     */
    public List<Map<String, String>> select(String sql, String fields) {

        List<Map<String, String>> rsList = new ArrayList<Map<String, String>>();

        // TODO 不使用string来读取参数
        String[] fieldsArr = fields.split(",");

        try {
            // 通过数据库的连接操作数据库，实现增删改查
            PreparedStatement ptmt = getConnection().prepareStatement(sql);

            // 执行 Sql 获取结果集
            ResultSet rs = ptmt.executeQuery();

            // 保存列中的字段集
            Map<String, String> rowData;
            // 遍历结果集
            while (rs.next()) {
                rowData = new HashMap<String, String>();
                for (String field : fieldsArr) {
                    rowData.put(field, rs.getString(field));
                }
                // 保存列的数据到行中
                rsList.add(rowData);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rsList;
    }

    /**
     * 获取查询的数据条数
     * <p>
     * <code>语句类似 SELECT COUNT(*) AS cn FROM tal</code>
     * </p>
     * 
     * @param sql
     * @return int
     * @throws SQLException
     */
    public int count(String sql) {
        int cn = 0;

        try {
            PreparedStatement ptmt = getConnection().prepareStatement(sql);
            ResultSet rs = ptmt.executeQuery();
            rs.next();
            cn = rs.getInt("cn");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return cn;

    }

    /**
     * 执行 Sql
     * 
     * @param sql
     * @return boolean
     * @throws SQLException
     */
    public boolean execute(String sql) {
        boolean rs = false;
        try {
            PreparedStatement ptmt = getConnection().prepareStatement(sql);
            rs = ptmt.execute(sql);
            ptmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 释放资源
     * 
     * @param conn
     * @param pstmt
     * @param rs
     */
    public void closeResources(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (null != pstmt) {
                    try {
                        pstmt.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        if (null != conn) {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
            }
        }
    }

}
