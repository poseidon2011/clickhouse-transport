package org.welyss.mysqlsync;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.welyss.mysqlsync.transport.DataSourceFactory;
import org.welyss.mysqlsync.transport.CHHandler;

@Component
public class TargetManager implements CommandLineRunner {

//	@Autowired
//	private Environment config;

	private final Logger log = LoggerFactory.getLogger(getClass());
	public Map<String, Target> targetPool = new HashMap<String, Target>();

	@Autowired
	private AutowireCapableBeanFactory beanFactory;
	@Autowired
	private DataSourceFactory cHDataSourceFactory;

	@Value("${sync.working.clickhouses}")
	private String clickhouses;

	/**
	 * 主入口
	 */
	@Override
	public void run(String... args) throws Exception {
		while (true) {
			Optional<String> chso = Optional.ofNullable(clickhouses);
			chso.ifPresent((chs -> {
				for (String ch : chs.split(",")) {
					// process target
					Target target;
					if (targetPool.containsKey(ch)) {
						target = targetPool.get(ch);
					} else {
						try {
							target = new Target();
							beanFactory.autowireBean(target);
							target.name = ch;
							target.tCHHandler = new CHHandler(ch, cHDataSourceFactory.take(ch));
							targetPool.put(ch, target);
						} catch (Exception e) {
							log.warn("cause exception when new target, msg: {}", e);
							continue;
						}
					}

					// process source
					try {
						List<Map<String, Object>> sources = target.tCHHandler.queryForMaps("SELECT id, sync_db, log_file, log_pos, log_timestamp FROM ch_syncdata_savepoints");
						for (int i = 0; i < sources.size(); i++) {
							Map<String, Object> sourceRow = sources.get(i);
							String syncDb = sourceRow.get("sync_db").toString();
							Map<String, Source> sourcePool = target.sourcePool;
							Source source;
							if (sourcePool.containsKey(syncDb)) {
								source = sourcePool.get(syncDb);
							} else {
								int id = Integer.parseInt(sourceRow.get("id").toString());
								source = new Source();
								source.id = id;
								source.name = syncDb;
								source.target = target;
								beanFactory.autowireBean(source);
								sourcePool.put(syncDb, source);
							}

							// process source table
							try {
								target.tCHHandler.queryForMaps("SELECT sync_table FROM ch_syncdata_detail WHERE sp_id=?", source.id).forEach((map)->{
									String table = map.get("sync_table").toString();
									Set<String> sync = source.syncTables;
									if (!sync.contains(table)) {
										sync.add(table);
									}
								});
							} catch (SQLException e) {
								log.warn("can't get sync tables. msg: {}", e);
								continue;
							}

							String logFile = sourceRow.get("log_file").toString();
							long logPos = Long.parseLong(sourceRow.get("log_pos").toString());
							Object logTimestampObj = sourceRow.get("log_timestamp");
							Long logTimestamp = null;
							if (logTimestampObj != null) {
								logTimestamp = Long.parseLong(logTimestampObj.toString());
							}
							source.start(logFile, logPos, logTimestamp);
						}
					} catch (Exception e) {
						log.warn("failed to get sources. msg: {}", e);
						continue;
					}
				}
			}));

			try {
				Thread.sleep(10 * 60 * 1000);
			} catch (Exception e) {
				log.warn("cause exception when target manager sleep, msg: {}", e);
			}
		}
	}
}
