package org.welyss.mysqlsync.db;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;


public interface MySQLHandler {
	public List<Map<String, Object>> queryForMaps(String sql, Object... args)
			throws SQLException;

	public int update(String sql, List<Object> params) throws SQLException;

	public int update(String sql, Object... params) throws SQLException;

	public int[] updateInTransaction(List<String> sqls,
			List<List<Object>> paramsList) throws SQLException;

	public String getJdbcUrl();

	public String getUsername();

	public String getPassword();

	public String getSchema();
}