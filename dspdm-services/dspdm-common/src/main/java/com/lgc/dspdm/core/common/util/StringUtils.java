package com.lgc.dspdm.core.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
    public static boolean isNullOrEmpty(String str) {
        return ((str == null) || (str.trim().length() == 0));
    }

    public static boolean hasValue(String str) {
        return !isNullOrEmpty(str);
    }

    public static boolean isWhiteSpace(String str) {
        return ((str != null) && (str.length() > 0) && (str.trim().length() == 0));
    }

    public static boolean isEmptyString(String str) {
        return ((str != null) && (str.trim().length() == 0));
    }

    public static boolean containsIgnoreCase(String container, String containee) {
        return ((container != null) && (container.length() > 0) && (containee != null) && (containee.length() > 0) && (container.toUpperCase().contains(containee.toUpperCase())));
    }

    public static String removeFromStartAndEnd(String value, String start, String end) {
        while (true) {
            if ((value.startsWith(start)) && (value.endsWith(end))) {
                value = value.substring((value.indexOf(start) + 1), value.lastIndexOf(end));
                value = value.trim();
            } else {
                break;
            }
        }
        return value;
    }
    /**
     * Formats a message by inserting the given aruguments to the place holders {}
     *
     * @param message
     * @param arguments
     * @return
     */
    public static String formatMessage(String message, Object... arguments) {
        String msg = message;
        if (CollectionUtils.hasValue(arguments)) {
            int index = 0;
            String replacement = null;
            while ((msg.contains("{}")) && (index < arguments.length)) {
                if (arguments[index] == null) {
                    replacement = "null";
                } else {
                    replacement = arguments[index].toString();
                }
                msg = msg.replaceFirst("\\Q{}\\E", Matcher.quoteReplacement(replacement));
                index++;
            }
        }
        return msg;
    }

    public static String makeFirstLetterCapital(String str) {
        String result = null;
        if (hasValue(str)) {
            result = str.trim();
            result = Character.toUpperCase(result.charAt(0)) + result.substring(1);
        }
        return result;
    }

    public static String toCamelCase(final String str) {
        if (str == null)
            return null;
        final StringBuilder ret = new StringBuilder(str.length());
        for (final String word : str.split(" ")) {
            if (!word.isEmpty()) {
                ret.append(Character.toUpperCase(word.charAt(0)));
                ret.append(word.substring(1).toLowerCase());
            }
            if (!(ret.length() == str.length()))
                ret.append(" ");
        }
        return ret.toString();
    }

    public static String getNextRandomNumber(int digitCount) {
        if ((digitCount < 0) || (digitCount > 10)) {
            throw new IllegalArgumentException("Invalid random number digit count : " + digitCount);
        }
        // %05.0f
        java.security.SecureRandom random = new java.security.SecureRandom();
        return String.format("%0" + digitCount + ".0f", (random.nextDouble() * Math.pow(10, digitCount)));
    }

    public static StackTraceElement[] parseExceptionStackTrace(String stackTrace) {
        // "at package.class.method(source.java:123)"
        Pattern tracePattern = Pattern
                .compile("\\s*at\\s+([\\w\\.$_]+)\\.([\\w$_]+)(\\(.*java)?:(\\d+)\\)(\\n|\\r\\n)");
        Matcher traceMatcher = tracePattern.matcher(stackTrace);
        List<StackTraceElement> stackTraceElements = new ArrayList<StackTraceElement>();
        while (traceMatcher.find()) {
            String className = traceMatcher.group(1);
            String methodName = traceMatcher.group(2);
            String sourceFile = traceMatcher.group(3);
            int lineNum = Integer.parseInt(traceMatcher.group(4));
            stackTraceElements.add(new StackTraceElement(className, methodName,
                    sourceFile, lineNum));
        }
        return stackTraceElements.toArray(new StackTraceElement[0]);
    }

    public static boolean isAlphaNumeric(String str) {
        return str.matches("^\\w+$");
    }

    public static boolean isAlphaNumericOrWhitespace(String str) {
        return str.matches("^[\\w\\-\\s]+$");
    }

    public static boolean isAlphaNumericOrUnderScore(String str) {
        return str.matches("^[\\w\\_]+$");
    }

    public static boolean isAlphaNumericOrWhitespaceUnderScore(String str) {
        return str.matches("^[\\w\\_\\s]+$");
    }

    public static String convertListToCommaSeparatedString(List<Object> list){
        String commaSeparatedString = "";
        if(CollectionUtils.hasValue(list)){
            StringBuilder stringBuilder = new StringBuilder();
            for(Object obj : list){
                if(obj instanceof String){
                    stringBuilder.append("'").append(obj).append("'").append(",");
                }else{
                    stringBuilder.append(obj).append(",");
                }
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            commaSeparatedString = stringBuilder.toString().replace("[", "").replace("]", "");
        }
        return commaSeparatedString;
    }

    /**
     * replaces all the occurrences of the second argument in the first argument with the third argument
     * @param value
     * @param replacee
     * @param replacer
     * @return
     */
    public static String replaceAllIgnoreCase(String value, String replacee, String replacer) {
        // for special characters, we need to replace with the configuration value we have.
        // and for non-special characters, to maintain the user provided case, we need to remove special characters.
        if(DSPDMConstants.SPECIAL_CHARACTER_USED_FOR_WAF_RULES_BYPASSING.contains(replacer)){
            String initialRegex = "(?i)";// This regex is used to ignore case
            return value.replaceAll(Matcher.quoteReplacement((initialRegex + replacee)), replacer);
        }else{
            return removeSpecialCharacters(value);
        }
    }

    public static String replaceAllIgnoreCaseForGenericString(String value, String replacee, String replacer) {
        String initialRegex = "(?i)";// This regex is used to ignore case
        return value.replaceAll(Matcher.quoteReplacement((initialRegex + replacee)), replacer);
    }

    public static String removeSpecialCharacters(String value){
        return  value.replaceAll(Matcher.quoteReplacement("$$$"),"");
    }
}
