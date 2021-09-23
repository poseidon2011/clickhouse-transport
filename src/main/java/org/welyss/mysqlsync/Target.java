package org.welyss.mysqlsync;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.welyss.mysqlsync.db.MySQLHandler;

public class Target {
	public static final String ENCODING_UTF_8 = "UTF-8";
	private MySQLHandler tMySQLHandler;
	private Map<String, Source> sourcePool;

	public Target(String name, String host, int port, String schema, String user, String password) {
//		tMySQLHandler = new MySQLHandlerImpl(name, host, port, schema, user, password);
	}

	public void start() {
//		sourcePool = new HashMap<String, Source>((int)(sources.size() / 0.6));
//		for (int i = 0; i < sources.length; i++) {
//			String source = sources[i][0];
//			String host = sources[i][1];
//			int port = Integer.parseInt(sources[i][2]);
//			String user = sources[i][3];
//			String password = sources[i][4];
//			String logFile = sources[i][5];
//			long logPos = Long.parseLong(sources[i][6]);
//			sourcePool.put(name, new Source(source, host, port, user, password, logFile, logPos, this));
//		}
		Iterator<Entry<String, Source>> sourceIt = sourcePool.entrySet().iterator();
		while(sourceIt.hasNext()) {
			Entry<String, Source> sourceEntry = sourceIt.next();
			sourceEntry.getValue().start();
		}
	}

	public void stop() {
		
	}
}
