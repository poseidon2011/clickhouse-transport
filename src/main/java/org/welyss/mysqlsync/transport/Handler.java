package org.welyss.mysqlsync.transport;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;


public interface Handler {
	public List<Map<String, Object>> queryForMaps(String sql, Object... args)
			throws SQLException;

	public int update(String sql, List<Object> params) throws SQLException;

	public int update(String sql, Object... params) throws SQLException;

	public int executeInTransaction(Map<String, List<List<Object>>> queue) throws SQLException;
}