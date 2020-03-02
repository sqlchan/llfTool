package util;

import org.apache.commons.lang.time.FastDateFormat;

public class Time {
    public static void main(String[] args) {
        FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
        String s = df.format(11l);
        System.out.println(s);
    }
}
