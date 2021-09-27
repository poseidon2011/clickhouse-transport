package org.welyss.mysqlsync;

import org.welyss.mysqlsync.interfaces.Parser;

public class Source {
	private String name;
	private Parser parser;
	
	public Source(String name, String logFile, long logPos, Target target) {
		this.name = name;
		parser = new BinlogParser(name, host, port, user, password, logFile, logPos, target);
	}

	public void start() {
		parser.start();
	}
}
