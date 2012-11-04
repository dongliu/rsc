package xrd;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ScanNameUtil {
    private static final String regex = "^.+?_[0-9]+[.][a-zA-Z]{3,4}+$";
    private static final Pattern suffix = Pattern.compile("[.][a-zA-Z]{3,4}+$");
    private static final Pattern number = Pattern.compile("[0-9]+$");
    private static final String empty = "";

    // parse the string into three parts: prefix, number, and suffix
    public static String[] parse(String name) {

        String[] output = new String[3];
        String noSuffix; 
        Matcher suffixMatcher = suffix.matcher(name);
        if (suffixMatcher.find()) {
            output[2] = suffixMatcher.group();
            noSuffix = suffixMatcher.replaceFirst(empty);
        } else {
        	noSuffix = name;
        }
        Matcher numMatcher = number.matcher(noSuffix);
        if (numMatcher.find()) {
            output[1] = numMatcher.group();
            output[0] = numMatcher.replaceFirst(empty);
        } else {
            output[0] = noSuffix;
        }
        return output;
    }

    public static boolean isValid(String name) {
        return name.matches(regex);
    }

}
