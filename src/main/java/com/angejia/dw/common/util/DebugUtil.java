package com.angejia.dw.common.util;

public class DebugUtil {

    public static void dump (Object obj,int i) {
        DebugUtil.print(obj);
        DebugUtil.exit(i);
    }

    public static void dump (Object obj) {
        DebugUtil.print(obj);
    }

    public static void exit(int i) {
        System.exit(i);
    }

    public static void print (Object obj) {
        System.out.println(DebugUtil.getType(obj));
        System.out.println(obj);
    }

    public static String getType(Object o){
       return o.getClass().toString();
    }
}
