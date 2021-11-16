package org.welyss.mysqlsync;

import java.util.List;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogParserListener;

public interface Parser {
	void start() throws Exception;
	void stop() throws Exception;
	void setBinlogEventListener(BinlogEventListener listener);
	void addParserListener(BinlogParserListener listener);
	List<BinlogParserListener> getParserListeners();

	String getLogFile();
	void setLogFile(String logFile);
	long getLogPos();
	void setLogPos(long logPos);
	long getSavepoint();
	void setSavepoint(long savepoint);
	Long getLogTimestamp();
	void setLogTimestamp(Long logTimestamp);
	boolean isRunning();
}
