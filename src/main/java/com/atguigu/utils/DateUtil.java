package com.atguigu.utils;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/30 21:35
 */
public class DateUtil {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static String date2String(Date date){
        return sdf.format(date);
    }
    public static Date string2date(String date) throws ParseException {
        Date time = null;
        try{
            time = sdf.parse(date);
        }catch (Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return time;
    }
    public static Long string2timestamp(String date){
        Long time = null;
        try{
            time = Timestamp.valueOf(date).getTime();
        }catch (Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return time;
    }
    public static Date timestam2date(Long timestamp){
        return new Date(timestamp);
    }
}
