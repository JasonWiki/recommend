package com.angejia.dw.common.util;

import java.io.*;

public class FileUtil {
    
    /**
     * 向文件写数据
     * @param file_name
     * @param data
     * @param append 是否追加
     */
    public static void fileOutputStream (String file_name,String data,boolean append)  {

        try {

            File f = new File(file_name);
            FileOutputStream fop = new FileOutputStream(f,append);

            //构建OutputStreamWriter对象,参数可以指定编码,默认为操作系统默认编码,windows上是gbk
            OutputStreamWriter writer = new OutputStreamWriter(fop, "UTF-8");

            //写入到缓冲区
            writer.append(data);

            //关闭写入流,同时会把缓冲区内容写入文件,所以上面的注释掉
            writer.close();

            //关闭输出流,释放系统资源
            fop.close();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
    }
    
    
    /**
     * 读取文件
     * @param file_name
     * @return String
     */
    public static String fileInputStream(String file_name){

        StringBuilder sb = new StringBuilder();

        try {
            File f = new File(file_name);

            //构建FileInputStream对象
            FileInputStream fip = new FileInputStream(f);

            // InputStreamReader 逐行读取六中的数据,编码与写入相同
            InputStreamReader reader = new InputStreamReader(fip, "UTF-8");

            //一行行读去文件数据
            while (reader.ready()) {
                sb.append((char) reader.read());
            }

            //关闭读取流
            reader.close();

            //关闭输出流,释放系统资源
            fip.close();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return sb.toString();
    }

    
    public static boolean deleteFile (String file_name) {
        boolean isDelete = false;
        try{
            
            File file = new File(file_name);
            
            if(file.delete()){
                isDelete = true;
            }else{
                isDelete = false;
            }
       
           }catch(Exception e){
       
            e.printStackTrace();
       
           }
        return isDelete;
    }
    
    public static void main(String[] args) {
        //FileUtil a = new FileUtil();
        //a.fileOutputStream("/tmp/aaa","中文输入");
        //a.fileOutputStream("/tmp/aaa","\r\n");
        //System.out.println(a.fileInputStream("/tmp/aaa"));
    }
}
