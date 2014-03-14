package edu.arizona.cs.syndicate.util;

import java.util.Calendar;

public class TimeUtils {
    public static long getCurrentTimeLong() {
        return Calendar.getInstance().getTimeInMillis();
    }
}
