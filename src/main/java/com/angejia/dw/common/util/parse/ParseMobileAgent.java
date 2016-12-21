package com.angejia.dw.common.util.parse;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


/**
 * 解析 accessLog 中的 Http 头的 数据
 *   app=i-broker;av=1.0.0;ccid=1;gcid=1;ch=A01;lng=121.526063;lat=31.219871;ip=;mac=None;net=WIFI;p=iOS;pm=iPhone 4S;osv=7.1;dvid=32A02E76EC8C-4D78-B331-201503251125;
 * 这个数据是 APP 请求的时候发送的
 */
public class ParseMobileAgent {
     /**
     * @param s sting
     * @param p pattern
     * @return
     */
    public static String evaluate(String s, String p) {
        if (s == null) { return ""; }
        String base_p = p+"=([^;]+)";

        String result = "";

        String first_result = parseAgent(s, ";"+base_p);//先执行严格匹配，防止取p的时候把app的值取出来

        if(first_result == ""){
            String second_result = parseAgent(s, base_p);
            result = second_result;
        }else{
            result = first_result;
        }

        return result;
    }
    public static String parseAgent(String s,String p){
        if (s == null) { return ""; }
        Pattern pattern = Pattern.compile(p);
        Matcher matcher=pattern.matcher(s);

        if(matcher.find()){
            return matcher.group(1);
        }
        return "";
    }
//    public static void main(String[] args){
//    	String s = "app=i-broker;av=1.0.0;ccid=1;gcid=1;ch=A01;lng=121.526063;lat=31.219871;ip=;mac=None;net=WIFI;p=iOS;pm=iPhone 4S;osv=7.1;dvid=32A02E76EC8C-4D78-B331-201503251125;";
//    	ParseMobileAgent obj = new ParseMobileAgent();
//    	System.out.println(obj.evaluate(s,"app"));//开头的值
//    	System.out.println(obj.evaluate(s,"p"));//取重复值
//    	System.out.println(obj.evaluate(s,"gcid"));//中间的值
//    	System.out.println(obj.evaluate(s,"dvid"));//结尾的值
//    	System.out.println(obj.evaluate(s,"ip"));//取空值
//    	System.out.println(obj.evaluate(s,"notexist"));//不存在的值
//    }
}
