package com.lgc.dspdm.core.common.util;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

public class LocaleUtils {
    private static Map<String, Locale> localeMap = null;
    private static Map<String, TimeZone> timeZoneMap = null;
    private static Map<Float, TimeZone> timeZoneOffsetMap = null;

    static {
        initializeLocales();
        initializeTimezones();
    }

    private static void initializeLocales() {
        Locale[] locales = Locale.getAvailableLocales();
        localeMap = new HashMap<>(locales.length);
        for (Locale locale : locales) {
            localeMap.put(locale.getLanguage(), locale);
        }
    }

    private static void initializeTimezones() {
        String[] signs = new String[]{"-", "+"};
        String[] hours = new String[]{"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
        String[] minutes = new String[]{"00", "15", "30"};
        timeZoneMap = new HashMap<>(hours.length * minutes.length * 2);
        timeZoneOffsetMap = new HashMap<>(hours.length * minutes.length * 2);
        String timezone = null;
        String GMT = "GMT";
        for (String sign : signs) {
            for (String hour : hours) {
                for (String minute : minutes) {
                    timezone = GMT + sign + hour + ":" + minute;
                    timeZoneMap.put(timezone, TimeZone.getTimeZone(timezone));
                    float timeZoneOffset = (float) TimeZone.getTimeZone(timezone).getRawOffset() / (3600 * 1000);
                    timeZoneOffsetMap.put(timeZoneOffset, TimeZone.getTimeZone(timezone));
                }
            }
        }
        timeZoneMap.put(GMT, TimeZone.getTimeZone(GMT));
    }

    public static Locale getLocale(String language) {
        return localeMap.get(language);
    }

    public static TimeZone getTimezone(String timezone) {
        return timeZoneMap.get(timezone);
    }

    public static TimeZone getTimezone(Float timezoneOffset) {
        return timeZoneOffsetMap.get(timezoneOffset);
    }

    public static String getTimezone(TimeZone timezone) {
        String tz = null;
        for (Map.Entry<String, TimeZone> entry : timeZoneMap.entrySet()) {
            if (entry.getValue().getRawOffset() == (timezone.getRawOffset())) {
                tz = entry.getKey();
                break;
            }
        }
        return tz;
    }
}
