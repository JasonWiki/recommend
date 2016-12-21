package com.angejia.dw.common.util.mysql

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import java.sql.{ DriverManager, ResultSet }
import com.mysql.jdbc.Driver

class MysqlClient(ip: String, user: String, pwd: String, db: String) extends Serializable {

    // Change to Your Database Config
    lazy val conn_str =
        "jdbc:mysql://" +
            ip + ":3306/" +
            db + "?" +
            "user=" + user +
            "&password=" + pwd +
            "&zeroDateTimeBehavior=convertToNull"

    // Setup the connection
    lazy val conn = DriverManager.getConnection(conn_str)

    // 查询
    def select(sql: String): ArrayBuffer[HashMap[String, Any]] = {
        try {
            // Configure to be Read Only
            val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

            // Execute Query
            val rs = statement.executeQuery(sql)

            val rsmd = rs.getMetaData
            val colNames = for (i <- 1 to rsmd.getColumnCount) yield rsmd.getColumnLabel(i)
            val result = ArrayBuffer[HashMap[String, Any]]()

            while (rs.next) {
                var row = new HashMap[String, Any];
                for (n <- colNames) {
                    row.put(n, rs.getObject(n))
                }
                result += row
            }

            rs.close()
            statement.close()

            result
        } finally {
            //conn.close
        }
    }

    /**
     * 执行
     */
    def exec(sql: String): Int = {
        try {
            val prep = conn.prepareStatement(sql)
            //prep.setString(1, "Nothing great was ever achieved without enthusiasm.")
            //prep.setString(2, "Ralph Waldo Emerson")
            prep.executeUpdate
        } finally {
            //conn.close
        }
    }
    
    def close() : Unit  = {
        conn.close()
    }
}