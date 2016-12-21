package com.angejia.dw.common.util;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

/**
 * Json 转换
 * @author Jason 
 *  JSONObject 就是 java.util.Map
 *  JSONArray 就是 java.util.List
 *  使用 Map 或 List 的标准操作访问它们
 */
public class JavaJsonUtil {
    
    
    /**
     * JsonStr {} 转换成  java.util.Map
     * SONObject 就是 java.util.Map
     * @param strJson
     * @return
     */
    public static JSONObject JsonStrToMap (String strJson) {
        JSONParser parser = new JSONParser();
        JSONObject obj = null; 

        try{
            obj = (JSONObject) parser.parse(strJson);
        } catch(ParseException pe){
            System.out.println("position: " + pe.getPosition());
            System.out.println(pe);
        }
        //System.out.println(obj);
        return obj;
    }


    /**
     * JsonStr [{},{}] 转换成  java.util.List
     * JSONArray 就是 java.util.List
     * @param strJson
     * @return
     */
    public static JSONArray JsonStrToArray(String strJson) {
        JSONParser parser = new JSONParser();
        JSONArray obj = null; 

        try{
            obj = (JSONArray) parser.parse(strJson);
        } catch(ParseException pe){
            System.out.println("position: " + pe.getPosition());
            System.out.println(pe);
        }
        //System.out.println(obj);
        return obj;
    }

    /**
     * JSONObject 转换成 json 字符串
     * @param obj
     * @return
     */
    public static String MapToJsonStr(JSONObject obj) {
        return obj.toJSONString();
    }

    /**
     * JSONArray 转换成 Json 字符串
     * @param obj
     * @return
     */
    public static String ArrayToJsonStr(JSONArray obj) {
        return obj.toJSONString();
    }

    public static void main(String[] args) {
        JSONObject obja = JavaJsonUtil.JsonStrToMap("{\"a\":\"1\"}");
        obja.put("a", "1");
        //JavaJsonUtil.JsonStrToArray("[{\"a\":\"1\"},{\"a\":\"1\"}]");
        //JavaJsonUtil.MapToJson();
        System.exit(0);
        
        JSONParser parser=new JSONParser();
        String s = "[0,{\"1\":{\"2\":{\"3\":{\"4\":[5,{\"6\":7}]}}}}]";
            try{
            Object obj = parser.parse(s);
            JSONArray array = (JSONArray)obj;
            System.out.println("The 2nd element of array");
            System.out.println(array.get(1));
            System.out.println();
            JSONObject obj2 = (JSONObject)array.get(1);
            obj2.put(2, "a");
            System.out.println("Field \"1\"");
            System.out.println(obj2.get(2));

            s = "{}";
            obj = parser.parse(s);
            System.out.println(obj);

            s= "[5,]";
            obj = parser.parse(s);
            System.out.println(obj);

            s= "[5,,2]";
            obj = parser.parse(s);
            System.out.println(obj);
        }catch(ParseException pe){
            System.out.println("position: " + pe.getPosition());
            System.out.println(pe);
        }
    }
    
    
}
