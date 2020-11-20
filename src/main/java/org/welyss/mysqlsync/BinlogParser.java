package org.welyss.mysqlsync;

import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.welyss.mysqlsync.interfaces.Parser;

import com.google.code.or.OpenReplicator;

public class BinlogParser implements Parser {
	private String name;
	private Task task;
	private Set<Pipeline> piplines;
	private OpenReplicator binlogParser;
	@Value("${base.server.id}")
	private int baseServerId;

	public BinlogParser(String name, Task task) {
		this.name = name;
		this.task = task;
		binlogParser = new OpenReplicator();
//		binlogParser.setHost(host);
//		binlogParser.setPort(port);
//		binlogParser.setUser(user);
//		binlogParser.setPassword(password);
		binlogParser.setThreadNm("Source parser[" + task.getName() + "-" + name + "]");
		binlogParser.setServerId(baseServerId + task.getId());
		binlogParser.setEncoding(Task.ENCODING_UTF_8);
//		binlogParser.setBinlogFileName(logFile);
//		binlogParser.setBinlogPosition(logPos);
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
