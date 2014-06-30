package org.tomdz.storm.esper.utils;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class Timing {

    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withZoneUTC();
    public static long toLong(String date)
    {
        long timestamp = 0;
        timestamp = formatter.parseMillis(date);
        return timestamp;
    }

    public static String toDateString(long timestamp)
    {
        return formatter.print(timestamp);
    }

    public static int timeOfTheDay(long timestamp) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis(timestamp);
        return cal.get(Calendar.HOUR_OF_DAY);
    }

}
