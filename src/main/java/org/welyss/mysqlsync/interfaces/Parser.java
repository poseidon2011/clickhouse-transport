package org.welyss.mysqlsync.interfaces;

import com.google.code.or.binlog.BinlogEventListener;

public interface Parser {
	void start() throws Exception;
	void stop() throws Exception;
	void setBinlogEventListener(BinlogEventListener listener);

	String getLogFile();
	void setLogFile(String logFile);
	long getLogPos();
	void setLogPos(long logPos);
	Long getLogTimestamp();
	void setLogTimestamp(Long logTimestamp);
}
