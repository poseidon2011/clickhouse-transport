package org.welyss.mysqlsync;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.beans.factory.annotation.Value;

public class CommonUtils {
	@Value("sync.buffer.size")
	public static Long bufferSize;
	private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	
	public static LocalDateTime parseDate(String dateTimeStr) {
		return LocalDateTime.parse(dateTimeStr, dtf);
	}
}
