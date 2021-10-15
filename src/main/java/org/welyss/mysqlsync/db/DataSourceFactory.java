package org.welyss.mysqlsync.db;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

@Component
public class DataSourceFactory {
	@Autowired
	private DataSourceConfig dataSourceConfig;
	private Map<String, DataSource> pool = new ConcurrentHashMap<String, DataSource>();

	/**
	 * @param name
	 * @return
	 * @throws Exception 
	 */
	public DataSource take(String name) throws Exception {
		DataSource ds = null;
		if (pool.containsKey(name)) {
			ds = pool.get(name);
		} else {
			Map<String, Map<String, String>> clickhouseMap = dataSourceConfig.getClickhouse();
			if (clickhouseMap.containsKey(name)) {
				Map<String, String> dbinfo = clickhouseMap.get(name);
				String host = dbinfo.get("host");
				String port = dbinfo.get("port");
				String username = dbinfo.get("username");
				String password = dbinfo.get("password");
				String schema = dbinfo.get("schema");
				if (schema == null) {
					schema = name;
				}
				String url = "jdbc:clickhouse://" + host + ":" + port + "/" + schema;
				ClickHouseProperties properties = new ClickHouseProperties();
//				properties.setClientName(name);
				properties.setUser(username);
				properties.setPassword(password);

				properties.setSessionId(name);
				properties.setMaxInsertBlockSize(valueOrDefaultLong(dbinfo.get("maxInsertBlockSize"), 33554432L));
				properties.setMaxTotal(valueOrDefaultInt(dbinfo.get("maxTotal"), 10));

				ds = new ClickHouseDataSource(url, properties);
				
				String cachePrepStmts = dbinfo.get("cachePrepStmts");
				String prepStmtCacheSize = dbinfo.get("prepStmtCacheSize");
				String prepStmtCacheSqlLimit = dbinfo.get("prepStmtCacheSqlLimit");
				config.setPoolName(name);
				config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + schema
						+ "?useUnbufferedInput=false&useSSL=false&rewriteBatchedStatements=true");
				config.setDriverClassName("com.mysql.jdbc.Driver");
				config.setUsername(username);
				config.setPassword(password);
				config.addDataSourceProperty("cachePrepStmts", valueOrDefault(cachePrepStmts, "true"));
				config.addDataSourceProperty("prepStmtCacheSize", valueOrDefault(prepStmtCacheSize, "250"));
				config.addDataSourceProperty("prepStmtCacheSqlLimit", valueOrDefault(prepStmtCacheSqlLimit, "2048"));
				config.setMinimumIdle(valueOrDefaultInt(dbinfo.get("minimumIdle"), 1));
				config.setMaximumPoolSize(valueOrDefaultInt(dbinfo.get("maximumPoolSize"), 5));
				config.setConnectionTestQuery(valueOrDefault(dbinfo.get("connectionTestQuery"), "SELECT 1"));
				if (dbinfo.containsKey("connectionInitSql")) {
					config.setConnectionInitSql(dbinfo.get("connectionInitSql"));
				}
				config.setIdleTimeout(valueOrDefaultLong(dbinfo.get("idleTimeout"), MINUTES.toMillis(10)));
				config.setKeepaliveTime(valueOrDefaultLong(dbinfo.get("keepaliveTime"), MINUTES.toMillis(1)));
				config.setLeakDetectionThreshold(
						valueOrDefaultLong(dbinfo.get("leakDetectionThreshold"), MINUTES.toMillis(5)));
				config.setMaxLifetime(valueOrDefaultLong(dbinfo.get("maxLifetime"), MINUTES.toMillis(30)));
				config.setValidationTimeout(valueOrDefaultLong(dbinfo.get("validationTimeout"), SECONDS.toMillis(5)));
				ds = BasicDataSourceFactory.createDataSource(null);
				pool.put(name, ds);
			}
		}
		return ds;
	}

	private static String valueOrDefault(String val, String def) {
		return val == null ? def : val;
	}

	private static int valueOrDefaultInt(String val, int def) {
		return val == null ? def : Integer.parseInt(val);
	}

	private static long valueOrDefaultLong(String val, long def) {
		return val == null ? def : Long.parseLong(val);
	}

	public HostInfo getHostInfo(String name) {
		HostInfo result = null;
		Map<String, Map<String, String>> clickhouseMap = dataSourceConfig.getClickhouse();
		if (clickhouseMap.containsKey(name)) {
			Map<String, String> dbinfo = clickhouseMap.get(name);
			String host = dbinfo.get("host");
			String port = dbinfo.get("port");
			String username = dbinfo.get("username");
			String password = dbinfo.get("password");
			String schema = dbinfo.get("schema");
			result = new HostInfo(host, port, username, password, schema == null ? name : schema);
		}
		return result;
	}

	public static class HostInfo {
		public String host;
		public String port;
		public String username;
		public String password;
		public String schema;

		public HostInfo(String host, String port, String username, String password, String schema) {
			this.host = host;
			this.port = port;
			this.username = username;
			this.password = password;
			this.schema = schema;
		}
	}
}
