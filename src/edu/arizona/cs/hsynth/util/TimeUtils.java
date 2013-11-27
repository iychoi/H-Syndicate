package edu.arizona.cs.hsynth.util;

import java.util.Calendar;

public class TimeUtils {
    public static long getCurrentTimeLong() {
        return Calendar.getInstance().getTimeInMillis();
    }
}
