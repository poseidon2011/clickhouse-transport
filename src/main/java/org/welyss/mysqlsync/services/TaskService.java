package org.welyss.mysqlsync.services;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.welyss.mysqlsync.transport.DataSourceFactory;
import org.welyss.mysqlsync.transport.MySQLHandler;

@Component
public class TaskService {
	private static final Logger log = LoggerFactory.getLogger(TaskService.class);
	@Autowired
	private DataSourceFactory dataSourceFactory;

	public String tableMetaConvert(String dbname, String tablenm) {
		StringBuilder meta = new StringBuilder();
		try {
			MySQLHandler handler = new MySQLHandler(dbname, dataSourceFactory.takeMysql(dbname));
			List<Map<String, Object>> columns = handler.queryForMaps("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
					dbname, tablenm);
			List<Map<String, Object>> uniKeys = handler.queryForMaps("SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY INDEX_NAME, SEQ_IN_INDEX) KEY_COL_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=? and TABLE_NAME = ? AND NON_UNIQUE = 0 GROUP BY INDEX_NAME ORDER BY IF(INDEX_NAME='PRIMARY', 1, 2) LIMIT 1",
					dbname, tablenm);
			if (columns.size() > 0) {
				meta.append("CREATE TABLE `").append(tablenm).append("`(\n");
				for (int i = 0; i < columns.size(); i++) {
					Map<String, Object> row = columns.get(i);
					String columnName = row.get("COLUMN_NAME").toString();
					String dataType = row.get("DATA_TYPE").toString();
					String columnType = row.get("COLUMN_TYPE").toString();
					meta.append("  `").append(columnName).append("` ");
					if (dataType.equals("mediumint") || dataType.equals("int") || dataType.equals("integer")) {
						if (columnType.indexOf("unsigned") >= 0) {
							meta.append("UInt32");
						} else {
							meta.append("Int32");
						}
					} else if (dataType.equals("bool") || dataType.equals("boolean")) {
						meta.append("Boolean");
					} else if (dataType.equals("tinyint")) {
						if (columnType.indexOf("unsigned") >= 0) {
							meta.append("UInt8");
						} else {
							meta.append("Int8");
						}
					} else if (dataType.equals("smallint")) {
						if (columnType.indexOf("unsigned") >= 0) {
							meta.append("UInt16");
						} else {
							meta.append("Int16");
						}
					} else if (dataType.equals("bigint")) {
						if (columnType.indexOf("unsigned") >= 0) {
							meta.append("UInt64");
						} else {
							meta.append("Int64");
						}
					} else if (dataType.equals("decimal") || dataType.equals("dec")) {
						meta.append("Decimal(").append(row.get("NUMERIC_PRECISION").toString()).append(", ").append(row.get("NUMERIC_SCALE").toString()).append(")");
					} else if (dataType.equals("float")) {
						meta.append("Float32");
					} else if (dataType.equals("double")) {
						meta.append("Float64");
					} else if (dataType.equals("date")) {
						meta.append("Date");
					} else if (dataType.equals("timestamp") || dataType.equals("datetime")) {
						meta.append("DateTime");
					} else {
						meta.append("String");
					}
					meta.append(",\n");
				}
				meta.deleteCharAt(meta.length() - 2);
//				meta.append(") ENGINE = ReplicatedReplacingMergeTree");
				meta.append(") ENGINE=ReplacingMergeTree");
				if (uniKeys.size()>0) {
					meta.append(" ORDER BY (");
					for (int i = 0; i < uniKeys.size(); i++) {
						Map<String, Object> row = uniKeys.get(i);
						String keyColName = row.get("KEY_COL_NAME").toString();
						meta.append(keyColName).append(",");
					}
					meta.deleteCharAt(meta.length() - 1);
					meta.append(")");
				}
			}
		} catch (Exception e) {
			log.error("cause exception when tableMetaConvert get", e);
		}
		return meta.toString();
	}
}
