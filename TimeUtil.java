package org.apache.flume.sink.hbase;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {
	public static String toTime(long millis, String format) {
		Date date = new Date(millis);
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		return formatter.format(date);
	}
	
	public static String toTime(long millis){
		return toTime(millis,"yyyy-MM-dd");
	}
}
