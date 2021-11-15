package org.welyss.mysqlsync;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogParserListener;

public class MyBinlogParser implements Parser {
	private OpenReplicator parser;
	public String logFile;
	public long logPos;
	public Long logTimestamp;

	public MyBinlogParser(int id, String name, String host, String port, String user, String password, String logFile, long logPos, Long logTimestamp) {
		parser = new OpenReplicator();
		parser.setHost(host);
		parser.setPort(Integer.parseInt(port));
		parser.setUser(user);
		parser.setPassword(password);
		parser.setThreadNm(name);
		parser.setServerId(id);
		parser.setEncoding(Target.ENCODING_UTF_8);
		parser.setBinlogFileName(logFile);
		parser.setBinlogPosition(logPos);
		this.logTimestamp = logTimestamp;
	}

	@Override
	public List<BinlogParserListener> getParserListeners() {
		return parser.getBinlogParser().getParserListeners();
	}

	@Override
	public void addParserListener(BinlogParserListener listener) {
		parser.getBinlogParser().addParserListener(listener);
	}

	public int getParserListenerCount() {
		return parser.getBinlogParser().getParserListeners().size();
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

	@Override
	public boolean isRunning() {
		return parser != null ? parser.isRunning() : false;
	}

}