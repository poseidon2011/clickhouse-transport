package org.welyss.mysqlsync.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

public class MySQLHandlerImpl implements MySQLHandler {
	public static final int SQL_EXP_DUPLI = 0x0426;
	public static final int SQL_EXP_TAB_NOT_EXISTS = 0x047A;

	private static final Logger LOGGER = LoggerFactory.getLogger(MySQLHandler.class);
	private HikariDataSource ds;
	private String name;

	public MySQLHandlerImpl(String name, HikariDataSource ds) {
		this.name = name;
		this.ds = ds;
	}

	@Override
	public String getJdbcUrl() { 
		return ds.getJdbcUrl();
	}

	@Override
	public String getPassword() {
		return ds.getPassword();
	}

	@Override
	public String getUsername() {
		return ds.getUsername();
	}

	@Override
	public String getSchema() {
		return ds.getSchema();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public List<Map<String, Object>> queryForMaps(String sql, Object... args) throws SQLException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try (Connection conn = getConnection(true)) {
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
		try (Connection conn = getConnection(true)) {
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
		try (Connection conn = getConnection(true)) {
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				for (int i = 0; i < params.length; i++) {
					Object param = params[i];
					ps.setObject(i + 1, param);
				}
				result = ps.executeUpdate();
			}
		}
		return result;
	}

	public int[] updateInTransaction(List<String> sqls, List<List<Object>> paramsList) throws SQLException {
		int[] result = null;
		try (Connection conn = getConnection(false)) {
			try {
				result = new int[sqls.size()];
				for (int i = 0; i < sqls.size(); i++) {
					String sql = sqls.get(i);
					List<Object> params = paramsList.get(i);
					try (PreparedStatement ps = conn.prepareStatement(sql)) {
						for (int j = 0; j < params.size(); j++) {
							Object param = params.get(j);
							ps.setObject(j + 1, param);
						}
						result[i] = ps.executeUpdate();
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

	private Connection getConnection(boolean autoCommit) throws SQLException {
		try {
			Connection conn = ds.getConnection();
			conn.setAutoCommit(autoCommit);
			return conn;
		} catch (Exception pee) {
			LOGGER.error("{}", pee);
			throw pee;
		}
	}
}
