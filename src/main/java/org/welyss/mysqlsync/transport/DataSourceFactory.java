package org.welyss.mysqlsync.transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

@Component
public class DataSourceFactory {
	public static final String KEY_DS_SCHEMA = "schema";
	public static final String KEY_DS_PASSWORD = "password";
	public static final String KEY_DS_USERNAME = "username";
	public static final String KEY_DS_PORT = "port";
	public static final String KEY_DS_HOST = "host";
	@Autowired
	private CHDataSourceConfig dataSourceConfig;
	private Map<String, ClickHouseDataSource> pool = new ConcurrentHashMap<String, ClickHouseDataSource>();
	private Map<String, DataSource> mysqlPool = new ConcurrentHashMap<String, DataSource>();

	/**
	 * @param name
	 * @return
	 * @throws Exception
	 */
	public ClickHouseDataSource take(String name) throws Exception {
		ClickHouseDataSource ds = null;
		if (pool.containsKey(name)) {
			ds = pool.get(name);
		} else {
			Map<String, Map<String, String>> clickhouseMap = dataSourceConfig.getDatasource();
			if (clickhouseMap.containsKey(name)) {
				Map<String, String> dbinfo = clickhouseMap.get(name);
				String host = dbinfo.get(KEY_DS_HOST);
				String port = dbinfo.get(KEY_DS_PORT);
				String username = dbinfo.get(KEY_DS_USERNAME);
				String password = dbinfo.get(KEY_DS_PASSWORD);
				String schema = dbinfo.get(KEY_DS_SCHEMA);
				if (schema == null) {
					schema = name;
				}
				String url = "jdbc:clickhouse://" + host + ":" + port + "/" + schema;
				ClickHouseProperties properties = new ClickHouseProperties();
//				properties.setClientName(name);
				properties.setUser(username);
				properties.setPassword(password);
//				properties.setDatabase(schema);
				properties.setSessionId(name);
				properties.setMaxInsertBlockSize(valueOrDefaultLong(dbinfo.get("maxInsertBlockSize"), 33554432L));
				properties.setMaxTotal(valueOrDefaultInt(dbinfo.get("maxTotal"), 10));
				properties.setTimeToLiveMillis(valueOrDefaultInt(dbinfo.get("timeToLiveMillis"), 10 * 60 * 1000));
				ds = new ClickHouseDataSource(url, properties);
				pool.put(name, ds);
			}
		}
		return ds;
	}

	/**
	 * @param name
	 * @return
	 * @throws Exception
	 */
	public DataSource takeMysql(String name) throws Exception {
		DataSource ds = null;
		if (mysqlPool.containsKey(name)) {
			ds = mysqlPool.get(name);
		} else {
			@SuppressWarnings("resource")
			BasicDataSource dsInit = new BasicDataSource();
			Map<String, Map<String, String>> clickhouseMap = dataSourceConfig.getDatasource();
			if (clickhouseMap.containsKey(name)) {
				Map<String, String> dbinfo = clickhouseMap.get(name);
				String host = dbinfo.get(KEY_DS_HOST);
				String port = dbinfo.get(KEY_DS_PORT);
				String username = dbinfo.get(KEY_DS_USERNAME);
				String password = dbinfo.get(KEY_DS_PASSWORD);
				String schema = dbinfo.get(KEY_DS_SCHEMA);
				if (schema == null) {
					schema = name;
				}
				String url = "jdbc:mysql://" + host + ":" + port + "/" + schema;
				String params = "?useUnicode=true&useSSL=false&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
				dsInit.setUrl(url + params);
				dsInit.setDriverClassName("com.mysql.jdbc.Driver");
				dsInit.setUsername(username);
				dsInit.setPassword(password);
				dsInit.setMaxIdle(1);
				dsInit.setMaxTotal(valueOrDefaultInt(dbinfo.get("maxTotal"), 2));
				ds = dsInit;
				mysqlPool.put(name, ds);
			}
		}
		return ds;
	}

//	private String valueOrDefault(String val, String def) {
//		return val == null ? def : val;
//	}

	private int valueOrDefaultInt(String val, int def) {
		return val == null ? def : Integer.parseInt(val);
	}

	private long valueOrDefaultLong(String val, long def) {
		return val == null ? def : Long.parseLong(val);
	}

	public HostInfo getHostInfo(String name) {
		HostInfo result = null;
		Map<String, Map<String, String>> clickhouseMap = dataSourceConfig.getDatasource();
		if (clickhouseMap.containsKey(name)) {
			Map<String, String> dbinfo = clickhouseMap.get(name);
			String host = dbinfo.get(KEY_DS_HOST);
			String port = dbinfo.get(KEY_DS_PORT);
			String username = dbinfo.get(KEY_DS_USERNAME);
			String password = dbinfo.get(KEY_DS_PASSWORD);
			String schema = dbinfo.get(KEY_DS_SCHEMA);
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
