package org.welyss.mysqlsync;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.welyss.mysqlsync.db.HikariDataSourceFactory;
import org.welyss.mysqlsync.db.HikariDataSourceFactory.HostInfo;
import org.welyss.mysqlsync.db.TableMetaCache.Table;
import org.welyss.mysqlsync.db.TableMetaCache.TableMeta;
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

import hbec.app.platform.sync.MySqlTaskHandler.ColumnInfo;
import hbec.app.platform.sync.MySqlTaskHandler.TargetTable;

public class Source {
	private static final String DEFAULT_MYSQL_PORT = "3306";
	private int id;
	private String name;
	private Long logTimestamp;
	private Parser parser;
	private Set<String> syncTables;

	public Source(int id, String name, String logFile, long logPos, Long logTimestamp, Target target) {
		this.id = id;
		this.name = name;
		this.logTimestamp = logTimestamp;
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
					if (syncTables.contains(abre.getTableName()) ) {
						long step1 = System.nanoTime();
						Map<String, List<List<Object>>> sqlTaskMap = new HashMap<String, List<List<Object>>>();
						TableMeta tableInfo = getTableInfo(dbAlias, abre.getTableName());
						
						if (tableInfo.targetTables != null) {
							for (TargetTable target : tableInfo.targetTables) {
								String table = target.name;
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
										if (columnLen - target.columns.size() == 1) {
											columnLen--;
										}
										for (Pair<Row> pair : rows) {
											boolean changed = false;
											StringBuilder sqlBuff = new StringBuilder();
											List<Object> params = new ArrayList<Object>();
											StringBuilder whereCause = new StringBuilder();
											List<Object> whereParams = new ArrayList<Object>();
											List<Column> before = pair.getBefore().getColumns();
											List<Column> after = pair.getAfter().getColumns();
											for (int i=0; i<columnLen; i++) {
												ColumnInfo columnInfo = target.columns.get(i);
												Object beforeVal = convertVal(tableInfo, columnInfo, before.get(i).getValue());
												Object afterVal = convertVal(tableInfo, columnInfo, after.get(i).getValue());
												if ((beforeVal == null && afterVal != null) || (beforeVal != null && !equareObj(beforeVal, afterVal))) {
													if (columnInfo.want) {
														if (sqlBuff.length() > 0) {
															sqlBuff.append(",");
														}
														sqlBuff.append(" `").append(columnInfo.name).append("` = ?");
														params.add(afterVal);
														changed = true;
													}
												}
												if (whereCause.length() > 0) {
													whereCause.append(" and ");
												}
												whereCause.append('`').append(columnInfo.name).append("` = ?");
												whereParams.add(beforeVal);
											}
											sqlBuff.insert(0, "` set").insert(0, table).insert(0, "update `").append(" where ");
											for (int i=0; i<target.unikey.size(); i++) {
												ColumnInfo columnInfo = target.unikey.get(i);
												if (i > 0) {
													whereCause.append(" and ");
												} else {
													whereCause = new StringBuilder();
													whereParams.clear();
												}
												whereCause.append('`').append(columnInfo.name).append("` = ?");
												whereParams.add(convertVal(tableInfo, columnInfo, before.get(columnInfo.order).getValue()));
											}
											sqlBuff.append(whereCause);
											params.addAll(whereParams);
											if (changed) {
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
														LOGGER.error("target table:[{}.{}] tableInfo.columns: {}=>[{}] compare with binlog.columns size: {}."
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
								LOGGER.debug("[{}] sqltype:[{}], 1-2:{} ns, 2-3:{} ns", dbAlias, type, step2 - step1, step3 - step2);
							}
						}
						synchronized (taskQuene) {
//							LOGGER.debug("task handler get lock.");
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
									LOGGER.debug("[{}] buffer full, executing, taskQuene.total is [{}].", dbAlias, taskQuene.total);
//									taskQuene.wait();
									execTask.execute();
								} catch (InterruptedException e) {
									LOGGER.error("cause an interrupted exception when taskQuene.wait, reason is [{}]", e);
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

	public void start() {
		try {
			parser.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Table takeTableMeta(String table) {
		
	}
}
