package org.apache.flume.source.taildirExt.util;

/**
 * Created by hj on 15/10/22.
 */
public class TimeUtil {
    public static long slowOper = 0L;

    public static String recordTime(String oper, Long startTime) {
        long time = currentTimeMs() - startTime;
        String s = "";
        if (time > slowOper) {
            s = String.format("[operate]:[%s], [%s ms]", oper, time);
        }
        return s;
    }

    public static long currentTimeMs() {
        return System.currentTimeMillis();
    }

}
