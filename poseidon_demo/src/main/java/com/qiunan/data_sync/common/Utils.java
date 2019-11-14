package com.qiunan.data_sync.common;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Utils {

    private static SimpleDateFormat sdfYMDH = new SimpleDateFormat("yyyyMMddHH");
    private static SimpleDateFormat sdfYMD = new SimpleDateFormat("yyyyMMdd");

    public static int[] getYYYYMMDDHHFromTs(long ts){
        int[] res = new int[4];
        Calendar calendar = new GregorianCalendar();
        calendar.setTimeInMillis(ts);
        res[0] = calendar.get(Calendar.YEAR);
        res[1] = calendar.get(Calendar.MONTH);
        res[2] = calendar.get(Calendar.DAY_OF_MONTH);
        res[3] = calendar.get(Calendar.HOUR_OF_DAY);

        return res;
    }

    public static int getHourFromTs(long ts){
        Date date = new Date(ts);
        synchronized(sdfYMDH) {
            return Integer.parseInt(sdfYMDH.format(date));
        }
    }

    public static int getDayFromTs(long ts){
        Date date = new Date(ts);
        synchronized(sdfYMD) {
            return Integer.parseInt(sdfYMD.format(date));
        }
    }
    
    public static long getNextDayBeginTsFromTs(long ts) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) + 1);
        return calendar.getTimeInMillis();
    }
    
}
