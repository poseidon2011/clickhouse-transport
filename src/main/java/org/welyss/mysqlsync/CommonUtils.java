package org.welyss.mysqlsync;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CommonUtils {
	private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	
	public static LocalDateTime parseDate(String dateTimeStr) {
		return LocalDateTime.parse(dateTimeStr, dtf);
	}
}
