package org.welyss.mysqlsync.services;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.welyss.mysqlsync.transport.CHHandler;
import org.welyss.mysqlsync.transport.DataSourceFactory;
import org.welyss.mysqlsync.transport.MySQLHandler;

@Component
public class TaskService {
	private static final Logger log = LoggerFactory.getLogger(TaskService.class);
	@Autowired
	private DataSourceFactory dataSourceFactory;

	public String createTable(String source, String tablenm, String target, String cluster) {
		String err = "done";
		try {
			String meta = generateTable(source, tablenm, target, cluster);
			CHHandler targetHandler = new CHHandler(target, dataSourceFactory.take(target));
			targetHandler.update(meta.toString());
			// savepoints
			List<Map<String, Object>> spList = targetHandler.queryForMaps("SELECT id FROM ch_syncdata_savepoints WHERE sync_db=?", source);
			if(spList.size()==0) {
				MySQLHandler sourceHandler = new MySQLHandler(source, dataSourceFactory.takeMysql(source));
				Map<String, Object> status = sourceHandler.queryForMaps("show master status").get(0);
				String logFile = status.get("File").toString();
				Long logPos = Long.parseLong(status.get("Position").toString());
				targetHandler.update("INSERT INTO ch_syncdata_savepoints SELECT max(id) + 1, ?, ?, ?, 0, now(), now() FROM ch_syncdata_savepoints", source, logFile, logPos);
			}
			// detail
			Integer spId = Integer.parseInt(targetHandler.queryForMaps("SELECT id FROM ch_syncdata_savepoints WHERE sync_db=?", source).get(0).get("id").toString());
			targetHandler.update("INSERT INTO ch_syncdata_detail SELECT max(id)+1, ?, ?, now() FROM ch_syncdata_detail", spId, tablenm);
		} catch (Exception e) {
			err = e.getMessage();
			if (err.matches(".*Table.*already exists.*")) {
				log.error("cause exception when createTable, msg: {}", err);
			} else {
				log.error("cause exception when createTable, msg: {}", err);
			}
		}
		return err;
	}

	public String generateTable(String source, String tablenm, String target, String cluster) throws Exception {
		StringBuilder meta = new StringBuilder();
		MySQLHandler sourceHandler = new MySQLHandler(source, dataSourceFactory.takeMysql(source));
		List<Map<String, Object>> columns = sourceHandler.queryForMaps("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
				source, tablenm);
		List<Map<String, Object>> uniKeys = sourceHandler.queryForMaps("SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY INDEX_NAME, SEQ_IN_INDEX) KEY_COL_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=? and TABLE_NAME = ? AND NON_UNIQUE = 0 GROUP BY INDEX_NAME ORDER BY IF(INDEX_NAME='PRIMARY', 1, 2) LIMIT 1",
				source, tablenm);
		if (columns.size() > 0) {
			CHHandler targetHandler = new CHHandler(target, dataSourceFactory.take(target));
			meta.append("CREATE TABLE `").append(targetHandler.getSchema()).append("`.`").append(tablenm).append("`");
			if (cluster != null) {
				meta.append(" ON CLUSTER ").append(cluster);
			}
			meta.append("(\n");
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
			if (cluster != null) {
				meta.append(") ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/share/{database}/{table}', '{shard}.{replica}')");
			} else {
				meta.append(") ENGINE=ReplacingMergeTree");
			}
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
		return meta.toString();
	}
}
