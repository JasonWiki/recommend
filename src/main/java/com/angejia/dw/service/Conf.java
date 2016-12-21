package com.angejia.dw.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.Map;
import java.util.HashMap;

import com.angejia.dw.common.util.PropertyUtil;

public class Conf {

    // 读取配置文件
    PropertyUtil property = new PropertyUtil();
    
    /**
     * 设置环境,根据不同的环境使用不同的配置文件
     * @throws IOException 
     */
    public void setEnv(String env)   {

        // 读取的配置文件名称
        String confName = "/conf_" + env + ".properties";

        // 获取 resource 文件读输入流
        InputStream  classPath = Conf.class.getResourceAsStream(confName);
        InputStreamReader inputStreamReader = new InputStreamReader(classPath);

        // 设置读取的流
        try {
            property.setFileInputStream(inputStreamReader);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    
    /**
     * 获取 Spark 配置
     * @return  Map<String,String>
     */
    public Map<String,String> getSparkConf(){
        Map<String, String> data = new HashMap<String, String>();
        data.put("sparkThriftServerUrl",  property.getKeyValue("spark.thrift.server.url"));
        data.put("sparkThriftServerUser",  property.getKeyValue("spark.thrift.server.user"));
        data.put("sparkThriftServerPass",  property.getKeyValue("spark.thrift.server.pass"));
        
        return data;
    }
    
    
    /**
     * 获取 elasticsearch 配置
     * @return
     */
    public Map<String,String> getElasticsearchMasterConf(){
        Map<String, String> data = new HashMap<String, String>();
        data.put("elasticsearchMasterHost",  property.getKeyValue("elasticsearch.master.host"));
        data.put("elasticsearchMasterPort",  property.getKeyValue("elasticsearch.master.port"));
        data.put("elasticsearchMasterCluster",  property.getKeyValue("elasticsearch.master.cluster"));
        return data;
    }
    
    
    /**
     * 获取 业务 mysql 配置
     * @return
     */
    public Map<String,String> getProductMysqDBInfo(){
        Map<String, String> data = new HashMap<String, String>();
        data.put("host",  property.getKeyValue("productMysqlDB.host"));
        data.put("account",  property.getKeyValue("productMysqlDB.account"));
        data.put("password",  property.getKeyValue("productMysqlDB.password"));
        data.put("defaultDB",  property.getKeyValue("productMysqlDB.defaultDB"));
        return data;
    }

    /**
     * 获取 dw mysql 配置
     * @return
     */
    public Map<String,String> getDwMysqDBInfo(){
        Map<String, String> data = new HashMap<String, String>();
        data.put("host",  property.getKeyValue("biMysqlDB.host"));
        data.put("account",  property.getKeyValue("biMysqlDB.account"));
        data.put("password",  property.getKeyValue("biMysqlDB.password"));
        data.put("defaultDB",  property.getKeyValue("biMysqlDB.defaultDB"));
        return data;
    }

}
