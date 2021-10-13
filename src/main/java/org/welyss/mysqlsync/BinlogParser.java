package org.welyss.mysqlsync;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;

public class BinlogParser implements Parser {
	private OpenReplicator parser;
	@Value("${base.server.id}")
	private int baseServerId;
	public String logFile;
	public long logPos;
	public Long logTimestamp;

	public BinlogParser(int id, String name, String host, String port, String user, String password, String logFile, long logPos, Long logTimestamp) {
		parser = new OpenReplicator();
		parser.setHost(host);
		parser.setPort(Integer.parseInt(port));
		parser.setUser(user);
		parser.setPassword(password);
		parser.setThreadNm(name);
		parser.setServerId(baseServerId + id);
		parser.setEncoding(Target.ENCODING_UTF_8);
		parser.setBinlogFileName(logFile);
		parser.setBinlogPosition(logPos);
		this.logTimestamp = logTimestamp;
	}

	@Override
	public void start() throws Exception {
		parser.start();
	}

	@Override
	public void stop() throws Exception {
		parser.stop(0, TimeUnit.SECONDS);
	}

	@Override
	public void setBinlogEventListener(BinlogEventListener listener) {
		parser.setBinlogEventListener(listener);
	}

	@Override
	public String getLogFile() {
		return logFile;
	}

	@Override
	public void setLogFile(String logFile) {
		this.logFile = logFile;
	}

	@Override
	public long getLogPos() {
		return logPos;
	}

	@Override
	public void setLogPos(long logPos) {
		this.logPos = logPos;
	}

	@Override
	public Long getLogTimestamp() {
		return logTimestamp;
	}

	@Override
	public void setLogTimestamp(Long logTimestamp) {
		this.logTimestamp = logTimestamp;
	}

}
