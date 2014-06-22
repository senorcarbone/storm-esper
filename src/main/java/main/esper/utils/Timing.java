package main.esper.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Timing {

    private static DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'", Locale.ENGLISH);

    static {
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public static long toLong(String date) {
        long timestamp = 0;
        try {
            timestamp = formatter.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return timestamp;
    }

    public static int timeOfTheDay(long timestamp) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis(timestamp);
        return cal.get(Calendar.HOUR_OF_DAY);
    }

    public static String toDateString(long timestamp) {
        return formatter.format(new Date(timestamp));
    }

}
