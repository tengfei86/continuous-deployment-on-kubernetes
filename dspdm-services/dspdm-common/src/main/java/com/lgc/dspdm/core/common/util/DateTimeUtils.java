package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.exception.DSPDMException;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

public class DateTimeUtils {
    public static final ZoneId ZONE_UTC = ZoneOffset.UTC.normalized();

    public static enum DISPLAY_PATTERNS {
        TIMESTAMP_ISO_FORMAT_WITH_TIMEZONE_AND_MILLIS("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        TIMESTAMP_FORMAT_WITH_TIMEZONE("yyyy-MM-dd HH:mm:ss.SSS XXX"),
        DATETIME_FORMAT("yyyy-MM-dd HH:mm:ssXXX"),
        DATE_ONLY_FORMAT("yyyy-MM-dd"),
        TIME_ONLY_FORMAT_WITH_MILLIS("HH:mm:ss.SSSX"),
        TIME_ONLY_FORMAT("HH:mm:ssXXX"),
        DATE_TIME_12_HOUR_FORMAT("yyyy-MM-dd hh:mm a"),
        TIME_ONLY_12_HOUR_FORMAT("hh:mm a");

        DISPLAY_PATTERNS(String pattern) {
            this.pattern = pattern;
        }

        private String pattern = null;

        public String getPattern() {
            return pattern;
        }
    }

    ;

    /**
     * define the values in below array in descending order. Put detailed pattern first for matching
     */
    private static enum PARSE_PATTERNS {
        TIME_ONLY_FORMAT("HH:mm:ss"),
        TIME_ONLY_FORMAT_WITH_MILLIS("HH:mm:ss.SSS"),
        // HYPHEN BASED DATE FORMATS
        DATE_ONLY_FORMAT("yyyy-MM-dd"),
        DATETIME_FORMAT_WITHOUT_SECONDS("yyyy-MM-dd'T'HH:mm"),
        DATETIME_FORMAT_WITH_T("yyyy-MM-dd'T'HH:mm:ss"),
        DATETIME_FORMAT("yyyy-MM-dd HH:mm:ss"),
        DATETIME_FORMAT_WITH_TIMEZONE("yyyy-MM-dd HH:mm:ssX"),
        DATETIME_FORMAT_WITH_TIMEZONE_FULL("yyyy-MM-dd HH:mm:ssXXX"),
        TIMESTAMP_FORMAT_WITHOUT_TIMEZONE("yyyy-MM-dd HH:mm:ss.SSS"),
        TIMESTAMP_FORMAT_WITH_TIMEZONE("yyyy-MM-dd HH:mm:ss.SSSX"),
        TIMESTAMP_ISO_FORMAT_UTC("yyyy-MM-dd'T'HH:mm:ss'Z'"),
        TIMESTAMP_ISO_FORMAT_WITH_MILLIS_UTC("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        TIMESTAMP_ISO_FORMAT_WITH_TIMEZONE("yyyy-MM-dd'T'HH:mm:ssX"),
        TIMESTAMP_ISO_FORMAT_WITH_TIMEZONE_AND_MILLIS("yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        // SLASH BASED DATE FORMATS
        DATE_ONLY_SLASH_FORMAT("yyyy/MM/dd"),
        DATETIME_SLASH_FORMAT_WITHOUT_SECONDS("yyyy/MM/dd'T'HH:mm"),
        DATETIME_SLASH_FORMAT_WITH_T("yyyy-MM-dd'T'HH:mm:ss"),
        DATETIME_SLASH_FORMAT("yyyy/MM/dd HH:mm:ss"),
        DATETIME_SLASH_FORMAT_WITH_TIMEZONE("yyyy/MM/dd HH:mm:ssX"),
        DATETIME_SLASH_FORMAT_WITH_TIMEZONE_FULL("yyyy/MM/dd HH:mm:ssXXX"),
        TIMESTAMP_SLASH_FORMAT_WITHOUT_TIMEZONE("yyyy/MM/dd HH:mm:ss.SSS"),
        TIMESTAMP_SLASH_FORMAT_WITH_TIMEZONE("yyyy/MM/dd HH:mm:ss.SSSX"),
        TIMESTAMP_ISO_SLASH_FORMAT_UTC("yyyy/MM/dd'T'HH:mm:ss'Z'"),
        TIMESTAMP_ISO_SLASH_FORMAT_WITH_MILLIS_UTC("yyyy/MM/dd'T'HH:mm:ss.SSS'Z'"),
        TIMESTAMP_ISO_SLASH_FORMAT_WITH_TIMEZONE("yyyy/MM/dd'T'HH:mm:ssX"),
        TIMESTAMP_ISO_SLASH_FORMAT_WITH_TIMEZONE_AND_MILLIS("yyyy/MM/dd'T'HH:mm:ss.SSSX");

        PARSE_PATTERNS(String pattern) {
            this.pattern = pattern;
        }

        private String pattern = null;

        private String getPattern() {
            return pattern;
        }

        /**
         * Any value should be parsed with detailed pattern first
         */
        private static PARSE_PATTERNS[] parseOrder = new PARSE_PATTERNS[]{
                TIMESTAMP_ISO_FORMAT_WITH_TIMEZONE_AND_MILLIS,
                TIMESTAMP_ISO_FORMAT_WITH_TIMEZONE,
                TIMESTAMP_ISO_FORMAT_WITH_MILLIS_UTC,
                TIMESTAMP_ISO_FORMAT_UTC,
                TIMESTAMP_FORMAT_WITH_TIMEZONE,
                TIMESTAMP_FORMAT_WITHOUT_TIMEZONE,
                DATETIME_FORMAT_WITH_TIMEZONE_FULL,
                DATETIME_FORMAT_WITH_TIMEZONE,
                DATETIME_FORMAT,
                DATETIME_FORMAT_WITH_T,
                DATETIME_FORMAT_WITHOUT_SECONDS,
                DATE_ONLY_FORMAT,
                // now checking date pattern with slashes
                TIMESTAMP_ISO_SLASH_FORMAT_WITH_TIMEZONE_AND_MILLIS,
                TIMESTAMP_ISO_SLASH_FORMAT_WITH_TIMEZONE,
                TIMESTAMP_ISO_SLASH_FORMAT_WITH_MILLIS_UTC,
                TIMESTAMP_ISO_SLASH_FORMAT_UTC,
                TIMESTAMP_SLASH_FORMAT_WITH_TIMEZONE,
                TIMESTAMP_SLASH_FORMAT_WITHOUT_TIMEZONE,
                DATETIME_SLASH_FORMAT_WITH_TIMEZONE_FULL,
                DATETIME_SLASH_FORMAT_WITH_TIMEZONE,
                DATETIME_SLASH_FORMAT,
                DATETIME_SLASH_FORMAT_WITH_T,
                DATETIME_SLASH_FORMAT_WITHOUT_SECONDS,
                DATE_ONLY_SLASH_FORMAT,
                // checking time only
                TIME_ONLY_FORMAT_WITH_MILLIS,
                TIME_ONLY_FORMAT
        };

        private static PARSE_PATTERNS[] getParseOrder() {
            return parseOrder;
        }
    }

    ;

    public static String getString(String format, java.sql.Timestamp timestamp, Locale locale, ZoneId withZoneId) {
        return DateTimeFormatter.ofPattern(format, locale).format(ZonedDateTime.of(timestamp.toLocalDateTime(), withZoneId));
    }

    public static String getString(java.util.Date utilDate, Locale locale, ZoneId zoneId) {
        String formatted = null;
        if (utilDate instanceof java.sql.Timestamp) {
            formatted = getStringTimestamp((java.sql.Timestamp) utilDate, locale, zoneId);
        } else if (utilDate instanceof java.sql.Date) {
            formatted = getStringDateOnly((java.sql.Date) utilDate, locale);
        } else if (utilDate instanceof java.sql.Time) {
            formatted = getStringTimeOnly((java.sql.Time) utilDate, locale, zoneId);
        } else {
            formatted = getStringDateTime(utilDate, locale, zoneId);
        }
        return formatted;
    }

    public static String getStringDateOnly(java.sql.Date sqlDate, Locale locale) {
        return DateTimeFormatter.ofPattern(DISPLAY_PATTERNS.DATE_ONLY_FORMAT.getPattern(), locale).format(sqlDate.toLocalDate());
    }

    private static ZoneOffset getOffset(ZoneId withZoneId) {
        ZoneOffset offset = ZoneOffset.UTC;
        if (withZoneId instanceof ZoneOffset) {
            offset = (ZoneOffset) withZoneId;
        } else if (withZoneId.normalized() instanceof ZoneOffset) {
            offset = (ZoneOffset) withZoneId.normalized();
        }
        return offset;
    }

    public static String getStringTimeOnly(java.sql.Time time, Locale locale, ZoneId withZoneId) {
        ZoneOffset offset = getOffset(withZoneId);
        return DateTimeFormatter.ofPattern(DISPLAY_PATTERNS.TIME_ONLY_FORMAT.getPattern(), locale).format(time.toLocalTime().atOffset(offset));
    }

    public static String getStringTimeOnly(String format, java.sql.Time time, Locale locale, ZoneId withZoneId) {
        ZoneOffset offset = getOffset(withZoneId);
        return DateTimeFormatter.ofPattern(format, locale).format(time.toLocalTime().atOffset(offset));
    }

    public static String getStringDateTime(java.util.Date utilDate, Locale locale, ZoneId withZoneId) {
        return DateTimeFormatter.ofPattern(DISPLAY_PATTERNS.DATETIME_FORMAT.getPattern(), locale).format(ZonedDateTime.of(new java.sql.Timestamp(utilDate.getTime()).toLocalDateTime(), withZoneId));
    }

    public static String getStringTimestamp(java.sql.Timestamp timestamp, Locale locale, ZoneId withZoneId) {
        return DateTimeFormatter.ofPattern(DISPLAY_PATTERNS.TIMESTAMP_ISO_FORMAT_WITH_TIMEZONE_AND_MILLIS.getPattern(), locale).format(ZonedDateTime.of(timestamp.toLocalDateTime(), withZoneId));
    }

    public static String getStringDBTimestamp(java.sql.Timestamp timestamp, Locale locale, ZoneId withZoneId) {
        return DateTimeFormatter.ofPattern(DISPLAY_PATTERNS.DATETIME_FORMAT.getPattern(), locale).format(ZonedDateTime.of(timestamp.toLocalDateTime(), withZoneId));
    }

    public static ZonedDateTime parse(String date, ExecutionContext executionContext) {
        if (StringUtils.isNullOrEmpty(date)) {
            throw new DSPDMException("Unable to parse the date is null", executionContext.getExecutorLocale());
        }

        TemporalAccessor temporalAccessor = null;
        // check for ISO standard format first
        try {
            temporalAccessor = DateTimeFormatter.ISO_ZONED_DATE_TIME.parseBest(date, ZonedDateTime::from, OffsetDateTime::from, LocalDateTime::from, LocalDate::from, LocalTime::from);
        } catch (Exception e) {
        }
        if (temporalAccessor == null) {
            // date is not parsed with standard ISO format then check other custom formats, if successful
            for (PARSE_PATTERNS parsePattern : PARSE_PATTERNS.getParseOrder()) {
                try {
                    temporalAccessor = DateTimeFormatter.ofPattern(parsePattern.getPattern(), executionContext.getExecutorLocale()).parse(date);
                    // break on successful parsing
                    break;
                } catch (Exception e) {
                }
            }
            if (temporalAccessor == null) {
                for (DISPLAY_PATTERNS displayPattern : DISPLAY_PATTERNS.values()) {
                    try {
                        temporalAccessor = DateTimeFormatter.ofPattern(displayPattern.getPattern(), executionContext.getExecutorLocale()).parse(date);
                        // break on successful parsing
                        break;
                    } catch (Exception e) {
                    }
                }
            }
        }

        ZonedDateTime zonedDateTime = null;
        if (temporalAccessor != null) {
            if (temporalAccessor instanceof ZonedDateTime) {
                zonedDateTime = (ZonedDateTime) temporalAccessor;
                // comment following line if we do not want to respect/regard the timezone value coming inside string
                zonedDateTime = convertZone(zonedDateTime, executionContext.getExecutorTimeZone());
            } else if (temporalAccessor instanceof OffsetDateTime) {
                zonedDateTime = ((OffsetDateTime) temporalAccessor).toZonedDateTime();
                // comment following line if we do not want to respect/regard the timezone value coming inside string
                zonedDateTime = convertZone(zonedDateTime, executionContext.getExecutorTimeZone());
            } else if (temporalAccessor instanceof LocalDateTime) {
                zonedDateTime = ZonedDateTime.of((LocalDateTime) temporalAccessor, executionContext.getExecutorTimeZone());
            } else if (temporalAccessor instanceof LocalDate) {
                zonedDateTime = ZonedDateTime.of((LocalDate) temporalAccessor, LocalTime.now(), executionContext.getExecutorTimeZone());
            } else if (temporalAccessor instanceof LocalTime) {
                zonedDateTime = ZonedDateTime.of(LocalDate.now(), (LocalTime) temporalAccessor, executionContext.getExecutorTimeZone());
            } else if (temporalAccessor instanceof java.sql.Timestamp) {
                zonedDateTime = ZonedDateTime.of(((java.sql.Timestamp) temporalAccessor).toLocalDateTime(), executionContext.getExecutorTimeZone());
            } else if (temporalAccessor instanceof java.sql.Date) {
                zonedDateTime = ZonedDateTime.of(((java.sql.Date) temporalAccessor).toLocalDate(), LocalTime.now(), executionContext.getExecutorTimeZone());
            } else if (temporalAccessor instanceof java.sql.Time) {
                zonedDateTime = ZonedDateTime.of(LocalDate.now(), ((java.sql.Time) temporalAccessor).toLocalTime(), executionContext.getExecutorTimeZone());
            } else if (temporalAccessor instanceof java.util.Date) {
                zonedDateTime = ZonedDateTime.of(LocalDateTime.ofInstant(((java.util.Date) temporalAccessor).toInstant(), executionContext.getExecutorTimeZone()), executionContext.getExecutorTimeZone());
            } else {
                LocalDate parsedLocalDate = null;
                try {
                    parsedLocalDate = temporalAccessor.query(LocalDate::from);
                } catch (Exception e) {
                }

                LocalTime parsedLocalTime = null;
                try {
                    parsedLocalTime = temporalAccessor.query(LocalTime::from);
                } catch (Exception e) {
                }
                ZoneId parsedZoneId = null;
                try {
                    parsedZoneId = temporalAccessor.query(ZoneId::from);
                } catch (Exception e) {
                }

                if ((parsedLocalDate != null) && (parsedLocalTime != null)) {
                    zonedDateTime = ZonedDateTime.of(parsedLocalDate, parsedLocalTime, ZoneId.systemDefault());
                    zonedDateTime = zonedDateTime.withZoneSameLocal((parsedZoneId != null) ? parsedZoneId : executionContext.getExecutorTimeZone());
                } else if (parsedLocalDate != null) {
                    // set zone to UTC when time is set to zero
                    zonedDateTime = ZonedDateTime.of(parsedLocalDate, LocalTime.of(0, 0), ZoneId.systemDefault());
                    zonedDateTime = zonedDateTime.withZoneSameLocal((parsedZoneId != null) ? parsedZoneId : executionContext.getExecutorTimeZone());
                } else if (parsedLocalTime != null) {
                    zonedDateTime = ZonedDateTime.of(LocalDate.now(), parsedLocalTime, ZoneId.systemDefault());
                    zonedDateTime = zonedDateTime.withZoneSameLocal((parsedZoneId != null) ? parsedZoneId : executionContext.getExecutorTimeZone());
                }
            }
        }
        if (zonedDateTime == null) {
            throw new DSPDMException("Unable to parse the date specfied '{}' with timezone '{}' and Locale '{}'", executionContext.getExecutorLocale(), date, executionContext.getExecutorTimeZone().getId(), executionContext.getExecutorLocale().getDisplayName());
        }
        return zonedDateTime;
    }


    /**
     * adjust timezone of the given timestamp from client timezone to UTC timezone
     * This method should be called exactly once on a timestamp value as soon as the request comes from the client
     *
     * @param clientTimezoneTimestamp timestamp object coming from the request in client timezone
     * @param executionContext        current user execution context containing the client timezone corresponding to the given timestamp
     * @return timestamp in UTC timezone
     * @Author Muhammad Imran Ansari
     * @date 2019-09-26
     */
    public static Timestamp convertTimezoneFromClientToUTC(java.sql.Timestamp clientTimezoneTimestamp, ExecutionContext executionContext) {
        // 1. create a system zoned date time instance from the given timestamp and the System timezone
        ZonedDateTime systemZonedDateTime = ZonedDateTime.ofInstant(clientTimezoneTimestamp.toInstant(), ZoneId.systemDefault());
        // 2. Change the timezone to Client timezone without effecting/changing the time value
        ZonedDateTime clientZonedDateTime = systemZonedDateTime.withZoneSameLocal(executionContext.getExecutorTimeZone());
        // 2. convert the timezone from client to the UTC
        ZonedDateTime utcZonedDateTime = DateTimeUtils.convertZoneToUTC(clientZonedDateTime);
        // 3. return the timestamp again
        return DateTimeUtils.getTimestamp(utcZonedDateTime);
    }

    /**
     * adjust timezone of the given timestamp from UTC timezone to client timezone
     * This method should be called exactly once on a timestamp value beore sending the response to the client
     *
     * @param utcTimezoneTimestamp timestamp object coming from the database in utc timezone
     * @param executionContext     current user execution context containing the client timezone to be used to the given timestamp
     * @return
     * @Author Muhammad Imran Ansari
     * @date 2019-09-26
     */
    public static Timestamp convertTimezoneFromUTCToClient(java.sql.Timestamp utcTimezoneTimestamp, ExecutionContext executionContext) {
        // 1. create a system zoned date time instance from the given timestamp and the System timezone
        ZonedDateTime systemZonedDateTime = ZonedDateTime.ofInstant(utcTimezoneTimestamp.toInstant(), ZoneId.systemDefault());
        // 2. Change the timezone to UTC without effecting/changing the time value
        ZonedDateTime utcZonedDateTime = systemZonedDateTime.withZoneSameLocal(DateTimeUtils.ZONE_UTC);
        // 3. convert the timezone from UTC to the client timezone
        ZonedDateTime clientZonedDateTime = DateTimeUtils.convertZone(utcZonedDateTime, executionContext.getExecutorTimeZone());
        // 3. return the timestamp again
        return DateTimeUtils.getTimestamp(clientZonedDateTime);
    }

    /**
     * Converts time value to the UTC zone from the timezone inside the zoned date time field
     *
     * @param zonedDateTime
     * @return
     */
    public static ZonedDateTime convertZoneToUTC(ZonedDateTime zonedDateTime) {
        return convertZone(zonedDateTime, ZONE_UTC);
    }

    /**
     * Converts time value to the client/given time zone from the timezone inside the zoned date time field
     *
     * @param zonedDateTime
     * @param zoneId
     * @return
     */
    public static ZonedDateTime convertZone(ZonedDateTime zonedDateTime, ZoneId zoneId) {
        return zonedDateTime.withZoneSameInstant(zoneId);
    }

    public static LocalDateTime getLocalDateTime(ZonedDateTime zonedDateTime) {
        return zonedDateTime.toLocalDateTime();
    }

    public static LocalDateTime getLocalDateTimeUTC(ZonedDateTime zonedDateTime) {
        return convertZoneToUTC(zonedDateTime).toLocalDateTime();
    }

    public static LocalDate getLocalDate(ZonedDateTime zonedDateTime) {
        return zonedDateTime.toLocalDate();
    }

    public static LocalDate getLocalDateUTC(ZonedDateTime zonedDateTime) {
        return convertZoneToUTC(zonedDateTime).toLocalDate();
    }

    public static LocalTime getLocalTime(ZonedDateTime zonedDateTime) {
        return zonedDateTime.toLocalTime();
    }

    public static LocalTime getLocalTimeUTC(ZonedDateTime zonedDateTime) {
        return convertZoneToUTC(zonedDateTime).toLocalTime();
    }

    public static Timestamp getTimestamp(ZonedDateTime zonedDateTime) {
        return Timestamp.valueOf(zonedDateTime.toLocalDateTime());
    }

    public static java.sql.Date getSQLDate(ZonedDateTime zonedDateTime) {
        return java.sql.Date.valueOf(zonedDateTime.toLocalDate());
    }

    public static java.util.Date getUtilDate(ZonedDateTime zonedDateTime) {
        return java.util.Date.from(zonedDateTime.toInstant());
    }

    public static java.sql.Time getTime(ZonedDateTime zonedDateTime) {
        return java.sql.Time.valueOf(zonedDateTime.toLocalTime());
    }

    public static ZonedDateTime getCurrentZonedDateTime() {
        return ZonedDateTime.now();
    }

    public static ZonedDateTime getCurrentUTCZonedDateTime() {
        return convertZoneToUTC(getCurrentZonedDateTime());
    }

    public static Timestamp getCurrentLocalDateTime() {
        return getTimestamp(getCurrentZonedDateTime());
    }


    public static Timestamp getCurrentTimestamp() {
        return getTimestamp(getCurrentZonedDateTime());
    }

    public static Timestamp getCurrentTimestampUTC() {
        return getTimestamp(getCurrentUTCZonedDateTime());
    }

    public static java.sql.Date getCurrentSQLDate() {
        return getSQLDate(getCurrentZonedDateTime());
    }

    public static java.sql.Date getCurrentSQLDateUTC() {
        return getSQLDate(getCurrentUTCZonedDateTime());
    }

    public static java.sql.Time getCurrentTime() {
        return getTime(getCurrentZonedDateTime());
    }

    public static java.sql.Time getCurrentTimeUTC() {
        return getTime(getCurrentUTCZonedDateTime());
    }

    public static java.util.Date getCurrentUtilDate() { return new java.util.Date();};

    public static java.util.Date getCurrentUtilDateUTC() {
        return new java.util.Date(getCurrentUTCZonedDateTime().toEpochSecond());
    }

    public static void main(String[] args) {
        //System.out.println(DateTimeUtils.getString("yyyy-MM-dd HH:mm:ss.SSS OOOO", new Timestamp(System.currentTimeMillis()), Locale.CHINA, ZoneId.systemDefault()));
        //System.out.println(DateTimeUtils.getString("yyyy-MM-dd HH:mm:ss.SSS XXX", new Timestamp(System.currentTimeMillis()), Locale.CHINA, ZoneId.systemDefault()));

        ZonedDateTime parse = parse("1984-03-08T06:00:00+06", ExecutionContext.getTestUserExecutionContext());

        System.out.println(DateTimeUtils.getString("yyyy-MM-dd HH:mm:ss.SSS XXX", DateTimeUtils.getTimestamp(parse), Locale.CHINA, ZoneId.systemDefault()));
    }
}
