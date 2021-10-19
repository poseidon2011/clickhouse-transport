package org.welyss.mysqlsync;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CommonUtils {
	public static Long bufferSize;
	
	public CommonUtils(@Value("${sync.buffer.size}") Long bufferSize) {
		CommonUtils.bufferSize = bufferSize;
	}

	private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	private static DateTimeFormatter dtfShort = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");

	public static LocalDateTime parseDate(String dateTimeStr) {
		return LocalDateTime.parse(dateTimeStr, dtf);
	}

	public static LocalDateTime parseDateShort(String dateTimeStr) {
		return LocalDateTime.parse(dateTimeStr, dtfShort);
	}
}
