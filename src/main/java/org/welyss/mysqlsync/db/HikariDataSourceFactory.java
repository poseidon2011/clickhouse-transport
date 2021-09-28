package org.welyss.mysqlsync.db;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.welyss.mysqlsync.DataSourceProperties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariDataSourceFactory {
	@Autowired
	private static DataSourceProperties dsProperties;

	/**
	 * @param name
	 * @return
	 */
	public static HikariDataSource create(String name) {
		HikariDataSource ds = null;
		Map<String, Map<String, String>> clickhouseMap = dsProperties.getClickhouse();
		if (clickhouseMap.containsKey(name)) {
			Map<String, String> dbinfo = clickhouseMap.get(name);
			HikariConfig config = new HikariConfig();
			String host = dbinfo.get("host");
			String port = dbinfo.get("port");
			String username = dbinfo.get("username");
			String password = dbinfo.get("password");
			String schema = dbinfo.get("schema");
			if (schema == null) {
				schema = name;
			}
			String cachePrepStmts = dbinfo.get("cachePrepStmts");
			String prepStmtCacheSize = dbinfo.get("prepStmtCacheSize");
			String prepStmtCacheSqlLimit = dbinfo.get("prepStmtCacheSqlLimit");
			config.setPoolName(name);
			config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + schema + "?useUnbufferedInput=false&useSSL=false&rewriteBatchedStatements=true");
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
			config.setLeakDetectionThreshold(valueOrDefaultLong(dbinfo.get("leakDetectionThreshold"), MINUTES.toMillis(5)));
			config.setMaxLifetime(valueOrDefaultLong(dbinfo.get("maxLifetime"), MINUTES.toMillis(30)));
			config.setValidationTimeout(valueOrDefaultLong(dbinfo.get("validationTimeout"), SECONDS.toMillis(5)));
			ds = new HikariDataSource(config);
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

	public static HostInfo getHostInfo(String name) {
		HostInfo result = null;
		Map<String, Map<String, String>> clickhouseMap = dsProperties.getClickhouse();
		if (clickhouseMap.containsKey(name)) {
			Map<String, String> dbinfo = clickhouseMap.get(name);
			String host = dbinfo.get("host");
			String port = dbinfo.get("port");
			String username = dbinfo.get("username");
			String password = dbinfo.get("password");
			result = new HostInfo(host, port, username, password);
		}
		return result;
	}

	public static class HostInfo {
		public String host;
		public String port;
		public String username;
		public String password;
		public HostInfo(String host, String port, String username, String password) {
			this.host = host;
			this.port = port;
			this.username = username;
			this.password = password;
		}
	}
}
