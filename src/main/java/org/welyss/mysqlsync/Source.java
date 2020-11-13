package org.welyss.mysqlsync;

import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.code.or.OpenReplicator;

@Component
public class Source {
	private String name;
	private Task task;
	private Set<Pipeline> piplines;
	private OpenReplicator binlogParser;
	@Value("${base.server.id}")
	private int baseServerId;

	public Source(String name, String host, int port, String user, String password, String logFile, long logPos, Task task) {
		this.name = name;
		this.task = task;
		binlogParser = new OpenReplicator();
		binlogParser.setHost(host);
		binlogParser.setPort(port);
		binlogParser.setUser(user);
		binlogParser.setPassword(password);
		binlogParser.setThreadNm("Source parser[" + task.getName() + "-" + name + "]");
		binlogParser.setServerId(baseServerId + task.getId());
		binlogParser.setEncoding(Task.ENCODING_UTF_8);
		binlogParser.setBinlogFileName(logFile);
		binlogParser.setBinlogPosition(logPos);
	}

	public void start() {

	}

	public void stop() {

	}
}
