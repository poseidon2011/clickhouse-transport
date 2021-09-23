package org.welyss.mysqlsync.db;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.welyss.mysqlsync.DataSourceProperties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariDataSourceFactory {
	@Autowired
	private DataSourceProperties dsProperties;
	public HikariDataSource create(String name) {
		HikariDataSource ds = null;
		Map<String, Map<String, String>> clickhouseMap = dsProperties.getClickhouse();
		if (clickhouseMap.containsKey(name)) {
//		host = config.get(CommonUtils.HEADER + db + CommonUtils.KEY_DB_HOST);
//		port = config.get(CommonUtils.HEADER + db + CommonUtils.KEY_DB_PORT);
//		username = config.get(CommonUtils.HEADER + db + CommonUtils.KEY_DB_USERNAME);
//		password = config.get(CommonUtils.HEADER + db + CommonUtils.KEY_DB_PASSWORD);
//		schema = config.get(CommonUtils.HEADER + db + CommonUtils.KEY_DB_SCHEMA);
			HikariConfig config = new HikariConfig();
			config.setJdbcUrl("jdbc:mysql://localhost:3306/test?useSSL=false");
			config.setUsername("devtest");
			config.setPassword("123123");
			config.addDataSourceProperty("cachePrepStmts", "true");
			config.addDataSourceProperty("prepStmtCacheSize", "250");
			config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			ds = new HikariDataSource(config);
//		String url = "jdbc:mysql://" + host + ":" + port + "/" + (schema == null ? schema = db : schema)
//				+ "?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true";
//		ds.setUrl(url);
//		ds.setUsername(user);
//		ds.setPassword(password);
//		ds.setDriverClassName("com.mysql.jdbc.Driver");
//		ds.setValidationQuery("SELECT 1");
//		ds.setInitialSize(Integer.parseInt(getPropertyValue(db, "initialsize", "1")));
//		ds.setMinIdle(Integer.parseInt(getPropertyValue(db, "minidle", "1")));
//		ds.setMaxIdle(Integer.parseInt(getPropertyValue(db, "maxidle", "2")));
//		ds.setMaxActive(Integer.parseInt(getPropertyValue(db, "maxactive", "5")));
//		ds.setTestWhileIdle(Boolean.parseBoolean(getPropertyValue(db, "testwhileidle", "true")));
//		ds.setTestOnBorrow(Boolean.parseBoolean(getPropertyValue(db, "testonborrow", "false")));
//		ds.setMinEvictableIdleTimeMillis(
//				Integer.parseInt(getPropertyValue(db, "minevictableidletimemillis", "60000")));
//		ds.setTimeBetweenEvictionRunsMillis(
//				Integer.parseInt(getPropertyValue(db, "timebetweenevictionrunsmillis", "30000")));
//		ds.setLogAbandoned(Boolean.parseBoolean(getPropertyValue(db, "logabandoned", "true")));
//		ds.setRemoveAbandoned(Boolean.parseBoolean(getPropertyValue(db, "removeabandoned", "true")));
//		ds.setRemoveAbandonedTimeout(Integer.parseInt(getPropertyValue(db, "removeabandonedtimeout", "7200")));
		}
		return ds;
	}
}
