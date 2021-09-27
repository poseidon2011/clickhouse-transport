package org.welyss.mysqlsync;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.welyss.mysqlsync.db.HikariDataSourceFactory;
import org.welyss.mysqlsync.db.MySQLHandler;
import org.welyss.mysqlsync.db.MySQLHandlerImpl;

public class Target {
	public static final String ENCODING_UTF_8 = "UTF-8";
	private MySQLHandler tMySQLHandler;
	private Map<String, Source> sourcePool;
	private final Logger Log = LoggerFactory.getLogger(getClass());

	public Target(String name) {
		tMySQLHandler = new MySQLHandlerImpl(name, HikariDataSourceFactory.create(name));
	}

	public void start() {
		try {
			List<Map<String, Object>> sources = tMySQLHandler.queryForMaps("", "");
			sourcePool = new HashMap<String, Source>((int)(sources.size() / 0.6)); 
			for (int i = 0; i < sources.size(); i++) {
				Map<String, Object> sourceRow = sources.get(i);
				String name = sourceRow.get("name").toString();
				String logFile = sourceRow.get("log_file").toString();
				long logPos = 4;
				if (sourceRow.containsKey("log_pos")) {
					logPos = Long.parseLong(sourceRow.get("log_pos").toString());
				}
				Source source = new Source(name, logFile, logPos, this);
				sourcePool.put(name, source);
				source.start();
			}
		} catch (SQLException e) {
			Log.error("{}", e);
			throw new RuntimeException("failed to get sources.", e);
		}
	}

	public void stop() {
		
	}
}
