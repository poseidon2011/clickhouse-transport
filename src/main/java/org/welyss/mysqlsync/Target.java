package org.welyss.mysqlsync;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.welyss.mysqlsync.db.HikariDataSourceFactory;
import org.welyss.mysqlsync.db.MySQLHandler;
import org.welyss.mysqlsync.db.MySQLHandlerImpl;

public class Target {
	public static final String ENCODING_UTF_8 = "UTF-8";
	private MySQLHandler tMySQLHandler;
	private Map<String, Source> sourcePool;

	public Target(String name) {
		tMySQLHandler = new MySQLHandlerImpl(name, HikariDataSourceFactory.create(name));
	}

	public void start() {
		try {
			List<Map<String, Object>> sources = tMySQLHandler.queryForMaps("", "");
			sourcePool = new HashMap<String, Source>((int)(sources.size() / 0.6)); 
			for (int i = 0; i < sources.size(); i++) {
				Map<String, Object> source = sources.get(i);
				String host = source.get("host").toString();
				int port = Integer.parseInt(sources[i][2]);
				String user = sources[i][3];
				String password = sources[i][4];
				String logFile = sources[i][5];
				long logPos = Long.parseLong(sources[i][6]);
				sourcePool.put(name, new Source(source, host, port, user, password, logFile, logPos, this));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		Iterator<Entry<String, Source>> sourceIt = sourcePool.entrySet().iterator();
		while(sourceIt.hasNext()) {
			Entry<String, Source> sourceEntry = sourceIt.next();
			sourceEntry.getValue().start();
		}
	}

	public void stop() {
		
	}
}
