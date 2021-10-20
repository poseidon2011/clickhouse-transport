package org.welyss.mysqlsync.transport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;

public class CHHandler implements Handler {
	public static final int SQL_EXP_DUPLI = 0x0426;
	public static final int SQL_EXP_TAB_NOT_EXISTS = 0x047A;

	private static final Logger log = LoggerFactory.getLogger(Handler.class);
	private ClickHouseDataSource ds;
	private String name;

	public CHHandler(String name, ClickHouseDataSource ds) {
		this.name = name;
		this.ds = ds;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSchema() {
		return ds.getDatabase();
	}

	@Override
	public List<Map<String, Object>> queryForMaps(String sql, Object... args) throws SQLException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try (ClickHouseConnection conn = ds.getConnection()) {
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				if (args != null && args.length > 0) {
					for (int i = 0; i < args.length; i++) {
						Object arg = args[i];
						ps.setObject(i + 1, arg);
					}
				}
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						Map<String, Object> record = new HashMap<String, Object>();
						ResultSetMetaData rsmd = rs.getMetaData();
						for (int i = 1; i <= rsmd.getColumnCount(); i++) {
							String columnName = rsmd.getColumnLabel(i);
							Object value = null;
							value = rs.getObject(columnName);
							record.put(columnName, value);
						}
						result.add(record);
					}
				}
			}
		}
		return result;
	}

	@Override
	public int update(String sql, List<Object> params) throws SQLException {
		int result = -1;
		try (Connection conn = ds.getConnection()) {
			conn.setAutoCommit(true);
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				if (params != null) {
					for (int i = 0; i < params.size(); i++) {
						Object param = params.get(i);
						ps.setObject(i + 1, param);
					}
				}
				result = ps.executeUpdate();
			}
		}
		return result;
	}

	@Override
	public int update(String sql, Object... params) throws SQLException {
		int result = -1;
		try (Connection conn = ds.getConnection()) {
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				for (int i = 0; i < params.length; i++) {
					Object param = params[i];
					ps.setObject(i + 1, param);
				}
				ps.addBatch();
//				result = ps.executeUpdate();
				result = ps.executeBatch()[0];
			}
		}
		return result;
	}

	@Override
	public int executeInTransaction(Map<String, List<List<Object>>> queue) throws SQLException {
		int result = 0;
		try (Connection conn = ds.getConnection()) {
			try {
				Iterator<Entry<String, List<List<Object>>>> queueIt = queue.entrySet().iterator();
				while (queueIt.hasNext()) {
					Entry<String, List<List<Object>>> row = queueIt.next();
					String sql = row.getKey();
					List<List<Object>> params = row.getValue();
					try (PreparedStatement ps = conn.prepareStatement(sql)) {
						for (int i = 0; i < params.size(); i++) {
							List<Object> param = params.get(i);
							for (int j = 0; j < param.size(); j++) {
								Object paramSingle = param.get(j);
								ps.setObject(j + 1, paramSingle);
							}
							ps.addBatch();
						}
						ps.executeBatch();
					}
				}
				conn.commit();
			} catch (SQLException e) {
				conn.rollback();
				throw e;
			}
		}
		return result;
	}
}
