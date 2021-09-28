package org.welyss.mysqlsync.interfaces;

import com.google.code.or.binlog.BinlogEventListener;

public interface Parser {
	void start() throws Exception;
	void stop() throws Exception;
	void setBinlogEventListener(BinlogEventListener listener);
}
