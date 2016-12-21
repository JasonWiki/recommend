package com.angejia.dw.common.util;

import java.security.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * 日期转换
 * @author Jason
 */
public class DateUtil {
    
    //格式化日期模式，按照需求增加
    public static final String SIMPLE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String SIMPLE_Y_M_D_FORMAT = "yyyy-MM-dd";
    public static final String SIMPLE_YMD_FORMAT = "yyyyMMdd";
    public static final String SIMPLE_hms_FORMAT = "HH:mm:ss";


    /**
     *  String -> Date
     * @param time
     * @param time_format
     * @return Date
     */
    public static Date StringToDate (String time,String time_format) {
        //设置格式化模式
        SimpleDateFormat format = new SimpleDateFormat(time_format);

        Date result = null; 
        try {
            Date t = format.parse(time);
            result = t;
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }


    /**
     * String -> Timestamp
     * @param time 时间字符串
     * @param time_format 需要格式化的格式
     * @return Long
     * @throws ParseException
     * example :
     *  DateUtil.StringToTimestamp("2010-06-25",DateUtil.SIMPLE_Y_M_D_FORMAT);
     */
    public static Long StringToTimestamp (String time,String time_format)  {
        //设置格式化模式
        SimpleDateFormat format = new SimpleDateFormat(time_format);

        Date t = DateUtil.StringToDate(time,time_format);

        return t.getTime();

    }


    /**
     * Timestamp -> Sting
     * @param timestamp
     * @param time_format
     * @return String
     * example :
     *  Long timestamp =  DateUtil.StringToTimestamp("2010-06-25 00:24:00",DateUtil.SIMPLE_FORMAT);
        DateUtil.TimestampToSting(timestamp,DateUtil.SIMPLE_FORMAT);
     */
    public static String TimestampToSting (Long timestamp,String time_format) {
        //根据时间戳拿到日期对象
        Date date = new Date(timestamp);

        //设置格式化模式
        SimpleDateFormat format = new SimpleDateFormat(time_format);

        //通过格式化对象，返回结果
        String result = format.format(date);

        return result;
        
    }


    /**
     * Timestamp -> Date
     * @param timestamp
     * @return Date
     */
    public static Date TimestampToDate (Long timestamp) {

        Date date = new Date(timestamp);

        return date;
    }

    /**
     * String -> FormatString
     * @param time
     * @param current_time_format
     * @param new_time_format
     * @return String
     * example :
     *  String date = DateUtil.StringToFormatString("2015-06-03", DateUtil.SIMPLE_YMD_FORMAT,DateUtil.SIMPLE_Y_M_D_FORMAT);
     */
    public static String StringToFormatString (String time,String current_time_format,String new_time_format ) {
        //装换为时间戳
        Long timestamp = DateUtil.StringToTimestamp(time,current_time_format);

        //转换成字符串
        String string = DateUtil.TimestampToSting(timestamp,new_time_format);

        return string;
    }


    /**
     * date -> string
     * @param date
     * @param time_format
     * @return
     */
    public static String DateToString (Date date,String time_format) {

        //设置格式化模式
        SimpleDateFormat simple_date_format = new SimpleDateFormat(time_format);

        //格式化日期
        String result = simple_date_format.format(date);

        return result;
    }


    /**
     * date -> Timestamp
     * @param date
     * @return Long
     */
    public static Long DateToTimestamp (Date date) {
        return date.getTime();
    }


    /**
     * 获取当前日期的 偏移天数
     * @param offset_day 
     * @return
     */
    public static Date getCalendarOffsetDateDay(int offset_day) {
        return DateUtil.calendarOffsetDateDay(offset_day,new Date());
    }

    /**
     * 获取指定日期的 偏移天数
     * @param offset_day
     * @param curDate
     * @return
     */
    public static Date getCalendarOffsetDateDay(int offset_day, Date curDate) {
        return DateUtil.calendarOffsetDateDay(offset_day,curDate);
    }

    /** 
     * 获取指定偏移日期
     * @param offset_day 偏移天数，-1 表示昨天 1明天  2 后天，以此类推
     * @param curDate 指定日期
     * @return Date
     */
    public static Date calendarOffsetDateDay (int offset_day, Date curDate) {
        Calendar c1 = Calendar.getInstance();

        c1.setTime(curDate);   // 设置当前日期
        c1.add(Calendar.DATE,offset_day);

        int year = c1.get(Calendar.YEAR); //获得年
        int month = c1.get(Calendar.MONTH) + 1;  // 获得月份
        int date = c1.get(Calendar.DATE); // 获得日期
        int hours = c1.get(Calendar.HOUR_OF_DAY); // 获得小时
        int minute = c1.get(Calendar.MINUTE); // 获得分钟
        int second = c1.get(Calendar.SECOND); // 获得秒
        int day_of_week = c1.get(Calendar.DAY_OF_WEEK); //获得星期几（注意（这个与Date类是不同的）：1代表星期日、2代表星期1、3代表星期二，以此类推）

        //Date
        return c1.getTime();
    }


    /**
     * 获取当前时间戳
     * 1436768318923
     */
    public static Long getNowTimestamp() {
        return System.currentTimeMillis();
    }

    
    /**
     * 获取当前时间 , 可以指定格式
     * @return
     */
    public static String getCurTime(String time_format) {
        return DateUtil.TimestampToSting(DateUtil.getNowTimestamp(),time_format);
    }
    

    
    
    
    
    
    public static Date addDateOneDay(Date date) {  
        if (null == date) {  
            return date;  
        }  
        Calendar c = Calendar.getInstance();  
        c.setTime(date);   //设置当前日期  
        c.add(Calendar.DATE, 1); //日期加1天  
//     c.add(Calendar.DATE, -1); //日期减1天  
        date = c.getTime();  
        return date;  
    } 

    public static void main (String[] args) throws ParseException {

        Long timestamp =  DateUtil.StringToTimestamp("2010-06-25 02:24:10",DateUtil.SIMPLE_FORMAT);
        String t2s = DateUtil.TimestampToSting(timestamp,DateUtil.SIMPLE_FORMAT);
        
        Date date = DateUtil.TimestampToDate(timestamp);
        System.out.println(t2s);

    }
}
