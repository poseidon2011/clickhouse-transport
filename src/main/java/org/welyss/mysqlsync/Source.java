package org.welyss.mysqlsync;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.welyss.mysqlsync.db.HikariDataSourceFactory;
import org.welyss.mysqlsync.db.HikariDataSourceFactory.HostInfo;
import org.welyss.mysqlsync.db.MySQLColumn;
import org.welyss.mysqlsync.db.MySQLTable;
import org.welyss.mysqlsync.db.TableMetaCache;
import org.welyss.mysqlsync.interfaces.Parser;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;

public class Source {
	private static final String DEFAULT_MYSQL_PORT = "3306";
	private int id;
	private String name;
	private Long logTimestamp;
	private Parser parser;
	private Set<String> syncTables;
	private final Logger Log = LoggerFactory.getLogger(getClass());
	private Target target;

	public Source(int id, String name, String logFile, long logPos, Long logTimestamp, Target target) {
		this.id = id;
		this.name = name;
		this.logTimestamp = logTimestamp;
		this.target = target;
		try {
			target.tMySQLHandler.queryForMaps("SELECT sync_table FROM ch_syncdata_detail WHERE sp_id=?", id).forEach((map)->{
				syncTables.add(map.get("sync_table").toString());
			});
		} catch (SQLException e) {
			throw new RuntimeException("can't get sync tables.", e);
		}

		HostInfo hostInfo = HikariDataSourceFactory.getHostInfo(name);
		parser = new BinlogParser(id, name + "-" + target.name, hostInfo.host, hostInfo.port == null ? DEFAULT_MYSQL_PORT : hostInfo.port, hostInfo.username, hostInfo.password, logFile, logPos);
		parser.setBinlogEventListener(new BinlogEventListener() {
			@Override
			public void onEvents(BinlogEventV4 event) {
//				long elapsed = System.currentTimeMillis();
				BinlogEventV4Header beh = event.getHeader();
				int type = beh.getEventType();
				if (MySQLConstants.WRITE_ROWS_EVENT == type
						|| MySQLConstants.WRITE_ROWS_EVENT_V2 == type
						|| MySQLConstants.UPDATE_ROWS_EVENT == type
						|| MySQLConstants.UPDATE_ROWS_EVENT_V2 == type
						|| MySQLConstants.DELETE_ROWS_EVENT == type
						|| MySQLConstants.DELETE_ROWS_EVENT_V2 == type) {
					AbstractRowEvent abre = (AbstractRowEvent) event;
					if (hostInfo.schema.equals(abre.getDatabaseName()) && syncTables.contains(abre.getTableName())) {
						long step1 = System.nanoTime();
						Map<String, List<List<Object>>> sqlTaskMap = new HashMap<String, List<List<Object>>>();
						MySQLTable table = TableMetaCache.get(abre.getDatabaseName(), abre.getTableName());
						long step2 = System.nanoTime();
						if (MySQLConstants.UPDATE_ROWS_EVENT == type
								|| MySQLConstants.UPDATE_ROWS_EVENT_V2 == type) {
							List<Pair<Row>> rows;
							// update
							if (MySQLConstants.UPDATE_ROWS_EVENT == type) {
								rows = ((UpdateRowsEvent) event).getRows();
							} else {
								rows = ((UpdateRowsEventV2) event).getRows();
							}
							if (rows.size() > 0) {
								int columnLen  = rows.get(0).getBefore().getColumns().size();
								// remove aliyun rds hidden primary key
								if (columnLen - table.columns.size() == 1) {
									columnLen--;
								}
								for (int i = 0; i < rows.size(); i++) {
									Pair<Row> pair = rows.get(i);
									boolean changed = false;
									StringBuilder whereCause = new StringBuilder();
									StringBuilder setCause = new StringBuilder();
									List<Object> params = new ArrayList<Object>();
									List<Object> whereParams = new ArrayList<Object>();
									List<Column> before = pair.getBefore().getColumns();
									List<Column> after = pair.getAfter().getColumns();
									Object[] formattedBefVals = new Object[before.size()];
									for (int j = 0; j < columnLen; j++) {
										MySQLColumn column = table.columns.get(j);
										Object beforeVal = formatVal(table, column, before.get(j).getValue());
										formattedBefVals[j] = beforeVal;
										Object afterVal = formatVal(table, column, after.get(j).getValue());
										if (!equalObj(beforeVal, afterVal)) {
											setCause.append("`").append(column.name).append("`=?,");
											params.add(afterVal);
											changed = true;
										}
									}
									if (table.uniqueKey.length > 0) {
										for (int j = 0; j < table.uniqueKey.length; j++) {
											int uniqIndex = table.uniqueKey[j];
											whereCause.append('`').append(table.columns.get(uniqIndex)).append("`=? and ");
											whereParams.add(formattedBefVals[uniqIndex]);
										}
									} else {
										for (int j = 0; j < columnLen; j++) {
											MySQLColumn column = table.columns.get(j);
											whereCause.append('`').append(column.name).append("`=? and ");
											whereParams.add(formattedBefVals[j]);
										}
									}
									setCause.setCharAt(setCause.length() - 1, ' ');
									whereCause.delete(whereCause.length() - 5, whereCause.length());
									String sql = "ALTER TABLE `" + table.name + "` UPDATE " + setCause + "WHERE " + whereCause;
								}
							}
						} else {
							List<Row> rows;
							if (MySQLConstants.WRITE_ROWS_EVENT == type
									|| MySQLConstants.WRITE_ROWS_EVENT_V2 == type) {
								// insert
								if (MySQLConstants.WRITE_ROWS_EVENT == type) {
									rows = ((WriteRowsEvent) event).getRows();
								} else {
									rows = ((WriteRowsEventV2) event).getRows();
								}
								if (rows.size() > 0) {
									int columnLen  = rows.get(0).getColumns().size();
									if (columnLen - target.columns.size() == 1) {
										columnLen--;
									}
									for (Row row : rows) {
										StringBuilder sqlBuff = new StringBuilder();
										StringBuilder selColumns = new StringBuilder();
										List<Object> params = new ArrayList<Object>();
										List<Column> columns = row.getColumns();
										for (int i=0; i<columnLen; i++) {
											try {
												ColumnInfo columnInfo = target.columns.get(i);
												if (columnInfo.want) {
													if (sqlBuff.length() > 0) {
														sqlBuff.append(",");
													}
													sqlBuff.append("?");
													params.add(convertVal(tableInfo, columnInfo, columns.get(i).getValue()));
													selColumns.append("`").append(columnInfo.name).append("`").append(",");
												}
											} catch (IndexOutOfBoundsException ioobe) {
												Iterator<ColumnInfo> itci = target.columns.iterator();
												StringBuilder sb = new StringBuilder();
												while (itci.hasNext()) {
													sb.append(itci.next().name).append(",");
												}
												Log.error("target table:[{}.{}] tableInfo.columns: {}=>[{}] compare with binlog.columns size: {}."
														, toDb.getSchema(), tableInfo.name, target.columns.size(), sb, columns.size());
												throw ioobe;
											}
										}
										selColumns.deleteCharAt(selColumns.length() - 1);
										sqlBuff.insert(0, " values(").insert(0, ")").insert(0, selColumns).insert(0, "` (").insert(0, table).insert(0, "insert into `").append(")");
										List<List<Object>> paramsTmp;
										if (sqlTaskMap.containsKey(sqlBuff.toString())) {
											paramsTmp = sqlTaskMap.get(sqlBuff.toString());
										} else {
											paramsTmp = new ArrayList<List<Object>>();
											sqlTaskMap.put(sqlBuff.toString(), paramsTmp);
										}
										paramsTmp.add(params);
									}
								}
							} else if (MySQLConstants.DELETE_ROWS_EVENT == type
									|| MySQLConstants.DELETE_ROWS_EVENT_V2 == type) {
								// delete
								if (MySQLConstants.DELETE_ROWS_EVENT == type) {
									rows = ((DeleteRowsEvent) event).getRows();
								} else {
									rows = ((DeleteRowsEventV2) event).getRows();
								}
								if (rows.size() > 0) {
									int columnLen  = rows.get(0).getColumns().size();
									if (columnLen - target.columns.size() == 1) {
										columnLen--;
									}
									for (Row row : rows) {
										StringBuilder sqlBuff = new StringBuilder();
										List<Object> params = new ArrayList<Object>();
										StringBuilder whereCause = new StringBuilder();
										List<Object> whereParams = new ArrayList<Object>();
										List<Column> columns = row.getColumns();
										if (target.unikey.size() > 0) {
											for (int i=0; i<target.unikey.size(); i++) {
												ColumnInfo columnInfo = target.unikey.get(i);
												if (i > 0) {
													whereCause.append(" and ");
												}
												whereCause.append('`').append(columnInfo.name).append("` = ?");
												whereParams.add(convertVal(tableInfo, columnInfo, columns.get(columnInfo.order).getValue()));
											}
										} else {
											for (int i=0; i<columnLen; i++) {
												ColumnInfo columnInfo = target.columns.get(i);
												Object val = convertVal(tableInfo, columnInfo, columns.get(i).getValue());
												if (whereCause.length() > 0) {
													whereCause.append(" and ");
												}
												whereCause.append('`').append(columnInfo.name).append("` = ?");
												whereParams.add(val);
											}
										}
										sqlBuff.append("delete from `").append(table).append("` where ");
										sqlBuff.append(whereCause);
										params.addAll(whereParams);
										List<List<Object>> paramsTmp;
										if (sqlTaskMap.containsKey(sqlBuff.toString())) {
											paramsTmp = sqlTaskMap.get(sqlBuff.toString());
										} else {
											paramsTmp = new ArrayList<List<Object>>();
											sqlTaskMap.put(sqlBuff.toString(), paramsTmp);
										}
										paramsTmp.add(params);
									}
								}
							}
						}
						long step3 = System.nanoTime();
						Log.debug("[{}] sqltype:[{}], 1-2:{} ns, 2-3:{} ns", dbAlias, type, step2 - step1, step3 - step2);

						synchronized (taskQuene) {
//							Log.debug("task handler get lock.");
							List<MySqlTask> taskQueneList = taskQuene.getQueneTables().get(tableInfo.name);
							// add task
							Iterator<Entry<String, List<List<Object>>>> isqlTaskMap = sqlTaskMap.entrySet().iterator();
							int size = 0;
							while(isqlTaskMap.hasNext()) {
								Entry<String, List<List<Object>>> entry = isqlTaskMap.next();
								String sql = entry.getKey();
								List<List<Object>> paramsList = entry.getValue();
								MySqlTask lstTask;
								if (taskQueneList.size() > 0 && (lstTask = taskQueneList.get(taskQueneList.size() - 1)).getSql().equals(sql)) {
									lstTask.getParams().addAll(paramsList);
								} else {
									if (taskQueneList.size() == 0) {
										fullupElapsed = elapsed;
									}
									taskQueneList.add(new MySqlTask(sql, paramsList));
								}
								size += paramsList.size();
							}
							logPos = beh.getNextPosition();
							logTimestamp = event.getHeader().getTimestamp();
							taskQuene.total += size;
							// exec
							if (taskQuene.total >= SyncHandler.cache) {
								try {
									Log.debug("[{}] buffer full, executing, taskQuene.total is [{}].", dbAlias, taskQuene.total);
//									taskQuene.wait();
									execTask.execute();
								} catch (InterruptedException e) {
									Log.error("cause an interrupted exception when taskQuene.wait, reason is [{}]", e);
									throw new RuntimeException(e);
								}
							}
							logTimestamp = beh.getTimestamp();
						} // lock end
					}
				}
			}
		});
	}

	private Object formatVal(MySQLTable table, MySQLColumn column, Object val) throws Exception {
		Object result = null;
		if (val != null) {
			if (column.type.equals("timestamp") || column.type.equals("datetime")) {
				if (val.getClass().isAssignableFrom(Date.class)) {
					result = val;
				} else {
					try {
						result = CommonUtils.parseDate(val.toString());
					} catch (DateTimeParseException e) {
						Log.warn("from is [{}], to is [{}], table is [{}], columnInfo.name is [{}], columnInfo.type is [{}], val of class is [{}], val is [{}], {}",
								name, target.name, table.name, column.name, column.type,
								val.getClass().getName(), val, e);
						result = val;
					}
				}
			} else if (column.type.endsWith("unsigned")) {
				if (column.type.startsWith("bigint(") && ((long) val & 0x8000000000000000l) != 0) {
					result = readUnsignedLong((long) val);
				} else if (column.type.startsWith("int(") && ((int) val & 0x80000000) != 0) {
					result = ((Long.parseLong(val.toString()))) & 0xffffffffL;
				} else if (column.type.startsWith("mediumint(") && ((int) val & 0x800000) != 0) {
					result = Integer.parseInt(Integer.toBinaryString((int) val).substring(8, 32), 2);
				} else if (column.type.startsWith("smallint(") && ((int) val & 0x8000) != 0) {
					result = Integer.parseInt(Integer.toBinaryString((int) val).substring(16, 32), 2);
				} else if (column.type.startsWith("tinyint(") && ((int) val & 0x80) != 0) {
					result = Integer.parseInt(Integer.toBinaryString((int) val).substring(24, 32), 2);
				} else {
					result = val;
				}
			} else {
				result = val;
			}
		}
		return result;
	}

	private BigDecimal readUnsignedLong(long value) {
		if (value >= 0)
			return new BigDecimal(value);
		long lowValue = value & 0x7fffffffffffffffL;
		return BigDecimal.valueOf(lowValue).add(BigDecimal.valueOf(Long.MAX_VALUE)).add(BigDecimal.valueOf(1));
	}

	private boolean equalObj(Object a, Object b) {
		boolean result = false;
		if (a == null && b == null) {
			result = true;
		} else if (a != null && b != null) {
			if (a instanceof byte[]) {
				result = Arrays.equals((byte[])a, (byte[])b);
			} else {
				result = a.equals(b);
			}
		}
		return result;
	}

	public void start() {
		try {
			parser.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
