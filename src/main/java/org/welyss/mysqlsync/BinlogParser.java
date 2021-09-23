package org.welyss.mysqlsync;

import org.springframework.beans.factory.annotation.Value;
import org.welyss.mysqlsync.interfaces.Parser;

import com.google.code.or.OpenReplicator;

public class BinlogParser implements Parser {
	private String name;
	private Target target;
	private OpenReplicator parser;
	@Value("${base.server.id}")
	private int baseServerId;

	public BinlogParser(String name, String host, int port, String user, String password, String logFile, long logPos, Target target) {
		this.name = name;
		this.target = target;
		parser = new OpenReplicator();
		parser.setHost(host);
		parser.setPort(port);
		parser.setUser(user);
		parser.setPassword(password);
//		parser.setThreadNm(name + "-" + target.getName());
//		parser.setServerId(baseServerId + target.getId());
		parser.setEncoding(Target.ENCODING_UTF_8);
		parser.setBinlogFileName(logFile);
		parser.setBinlogPosition(logPos);
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}
}
