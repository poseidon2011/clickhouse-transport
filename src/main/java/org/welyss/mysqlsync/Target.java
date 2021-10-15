package org.welyss.mysqlsync;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.welyss.mysqlsync.db.DataSourceFactory;
import org.welyss.mysqlsync.db.MySQLHandler;
import org.welyss.mysqlsync.db.MySQLHandlerImpl;

public class Target {
	public static final String ENCODING_UTF_8 = "UTF-8";
	public String name;
	public MySQLHandler tMySQLHandler;
	private Map<String, Source> sourcePool;
	private final Logger log = LoggerFactory.getLogger(getClass());

	public Target(String name) {
		this.name = name;
		tMySQLHandler = new MySQLHandlerImpl(name, DataSourceFactory.take(name));
	}

	public void start() {
		log.info("target: {} start.", name);
		try {
			List<Map<String, Object>> sources = tMySQLHandler.queryForMaps("SELECT id, sync_db, log_file, log_pos, log_timestamp FROM ch_syncdata_savepoints");
			sourcePool = new HashMap<String, Source>((int) (sources.size() / 0.6));
			for (int i = 0; i < sources.size(); i++) {
				Map<String, Object> sourceRow = sources.get(i);
				int id = Integer.parseInt(sourceRow.get("id").toString());
				String syncDb = sourceRow.get("sync_db").toString();
				String logFile = sourceRow.get("log_file").toString();
				long logPos = Long.parseLong(sourceRow.get("log_pos").toString());
				Object logTimestampObj = sourceRow.get("log_pos");
				Long logTimestamp = null;
				if (logTimestampObj != null) {
					logTimestamp = Long.parseLong(logTimestampObj.toString());
				}
				Source source = new Source(id, syncDb, logFile, logPos, logTimestamp, this);
				sourcePool.put(syncDb, source);
				source.start();
			}
		} catch (SQLException e) {
			log.error("{}", e);
			throw new RuntimeException("failed to get sources.", e);
		}
	}

	public void stop() {

	}
}
