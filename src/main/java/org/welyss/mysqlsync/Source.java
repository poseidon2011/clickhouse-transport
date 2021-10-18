package org.welyss.mysqlsync;

import java.math.BigDecimal;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.welyss.mysqlsync.db.CHDataSourceFactory;
import org.welyss.mysqlsync.db.CHDataSourceFactory.HostInfo;
import org.welyss.mysqlsync.db.CHExecutor;
import org.welyss.mysqlsync.db.MySQLColumn;
import org.welyss.mysqlsync.db.MySQLQueue;
import org.welyss.mysqlsync.db.MySQLTable;
import org.welyss.mysqlsync.db.TableMetaCache;

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

@Component
public class Source {
	private static final String DEFAULT_MYSQL_PORT = "3306";
	protected Integer id;
	protected String name;
	protected Target target;
	protected Set<String> syncTables = new HashSet<String>();
	private Parser parser;
	private final Logger log = LoggerFactory.getLogger(getClass());
	private CHExecutor chExecutor;
	private boolean running = false;

	@Autowired
	private CHDataSourceFactory dataSourceFactory;
	@Autowired
	private TableMetaCache tableMetaCache;

	private void addQueue(String sql, List<Object> params, Map<String, List<Object[]>> queue) {
		if (queue.containsKey(sql)) {
			queue.get(sql).add(params.toArray());
		} else {
			List<Object[]> tmpParams = new ArrayList<Object[]>();
			tmpParams.add(params.toArray());
			queue.put(sql, tmpParams);
		}
	}

	private Object formatVal(MySQLTable table, MySQLColumn column, Object val) {
		Object result = null;
		if (val != null) {
			if (column.type.equals("timestamp") || column.type.equals("datetime")) {
				if (val.getClass().isAssignableFrom(Date.class)) {
					result = val;
				} else {
					try {
						result = CommonUtils.parseDate(val.toString());
					} catch (DateTimeParseException e) {
						log.warn("from is [{}], to is [{}], table is [{}], columnInfo.name is [{}], columnInfo.type is [{}], val of class is [{}], val is [{}], {}",
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

	private boolean confict(byte last, byte current) {
		return (last == MySQLQueue.QUERY_TYPE_INSERT && (current == MySQLQueue.QUERY_TYPE_UPDATE || current == MySQLQueue.QUERY_TYPE_DELETE))
				|| (last == MySQLQueue.QUERY_TYPE_DELETE && current == MySQLQueue.QUERY_TYPE_INSERT);
	}

	public void start(String logFile, long logPos, long logTimestamp) throws Exception {
		log.info("Source: {}-{} start.", name, target.name);
		HostInfo hostInfo = dataSourceFactory.getHostInfo(name);
		parser = new BinlogParser(id, name + "-" + target.name, hostInfo.host, hostInfo.port == null ? DEFAULT_MYSQL_PORT : hostInfo.port, hostInfo.username, hostInfo.password, logFile, logPos, logTimestamp);
		parser.setBinlogEventListener(new BinlogEventListener() {
			@Override
			public void onEvents(BinlogEventV4 event) {
//					long elapsed = System.currentTimeMillis();
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
						byte sqlType = -1;
						MySQLTable table;
						try {
							table = tableMetaCache.get(name, abre.getTableName());
							String sql = null;
							List<Object> params = new ArrayList<Object>();
							if (MySQLConstants.UPDATE_ROWS_EVENT == type || MySQLConstants.UPDATE_ROWS_EVENT_V2 == type) {
								// update
								sqlType = MySQLQueue.QUERY_TYPE_UPDATE;
								List<Pair<Row>> rows = MySQLConstants.UPDATE_ROWS_EVENT == type ? ((UpdateRowsEvent) event).getRows() : ((UpdateRowsEventV2) event).getRows();
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
										if (changed) {
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
											sql = "ALTER TABLE `" + table.name + "` UPDATE " + setCause + "WHERE " + whereCause;
										}
									}
								}
							} else if (MySQLConstants.WRITE_ROWS_EVENT == type || MySQLConstants.WRITE_ROWS_EVENT_V2 == type) {
								// insert
								sqlType = MySQLQueue.QUERY_TYPE_INSERT;
								List<Row> rows = MySQLConstants.WRITE_ROWS_EVENT == type ? ((WriteRowsEvent) event).getRows() : ((WriteRowsEventV2) event).getRows();
								if (rows.size() > 0) {
									int columnLen  = rows.get(0).getColumns().size();
									if (columnLen - table.columns.size() == 1) {
										columnLen--;
									}
									for (int i = 0; i < rows.size(); i++) {
										Row row = rows.get(i);
										StringBuilder selColumns = new StringBuilder();
										StringBuilder sqlVals = new StringBuilder();
										List<Column> columns = row.getColumns();
										for (int j=0; j<columnLen; j++) {
											try {
												MySQLColumn column = table.columns.get(j);
												params.add(formatVal(table, column, columns.get(j).getValue()));
												sqlVals.append("?,");
												selColumns.append("`").append(column.name).append("`,");
											} catch (IndexOutOfBoundsException ioobe) {
												Iterator<MySQLColumn> itci = table.columns.iterator();
												StringBuilder sb = new StringBuilder();
												while (itci.hasNext()) {
													sb.append(itci.next().name).append(",");
												}
												log.error("target table:[{}.{}] tableInfo.columns: {}=>[{}] compare with binlog.columns size: {}."
														, target.tMySQLHandler.getSchema(), table.name, table.columns.size(), sb, columns.size());
												throw ioobe;
											}
										}
										selColumns.deleteCharAt(selColumns.length() - 1);
										sqlVals.deleteCharAt(sqlVals.length() - 1);
										sql = "INSERT INTO `" + table.name + "`(" + selColumns + ")VALUES(" + sqlVals + ")";
									}
								}
							} else if (MySQLConstants.DELETE_ROWS_EVENT == type || MySQLConstants.DELETE_ROWS_EVENT_V2 == type) {
								// delete
								sqlType = MySQLQueue.QUERY_TYPE_DELETE;
								List<Row> rows = MySQLConstants.DELETE_ROWS_EVENT == type ? ((DeleteRowsEvent) event).getRows() : ((DeleteRowsEventV2) event).getRows();
								if (rows.size() > 0) {
									int columnLen  = rows.get(0).getColumns().size();
									if (columnLen - table.columns.size() == 1) {
										columnLen--;
									}
									for (int i = 0; i < rows.size(); i++) {
										Row row = rows.get(i);
										StringBuilder whereCause = new StringBuilder();
										List<Column> columns = row.getColumns();
										for (int j=0; j<table.uniqueKey.length; j++) {
											MySQLColumn column = table.columns.get(table.uniqueKey[j]);
											if (j > 0) {
												whereCause.append(" and ");
											}
											whereCause.append('`').append(column.name).append("`=?");
											params.add(formatVal(table, column, columns.get(column.order).getValue()));
										}
										sql = "ALTER TABLE `" + table.name + "` DELETE WHERE " + whereCause;
									}
								}
							}
							// get query and ready to execute
							MySQLQueue queue;
							Map<String, MySQLQueue> queues = chExecutor.getQuerys();
							if (queues.containsKey(table.name)) {
								queue = queues.get(table.name);
							} else {
								queue = new MySQLQueue();
								queues.put(table.name, queue);
							}
							// exec
							if (confict(queue.lastType, sqlType)) {
								try {
									log.debug("[{}-{}: {}] buffer full, executing, taskQuene.total is [{}].", name, target.name, table.name, queue.count);
									chExecutor.execute(queue);
									chExecutor.savepoint(parser.getLogPos(), parser.getLogTimestamp(), id);
								} catch (Exception e) {
									log.error("cause an exception when chExecutor.exec, reason is [{}]", e);
									throw new RuntimeException(e);
								}
							}
							if (sql != null) {
								// lock start
								synchronized (queue) {
									log.debug("task[{}-{}-{}] in source lock.", name, target.name, table.name);
									// add task
									Map<String, List<Object[]>> query = null;
									switch (sqlType) {
									case MySQLQueue.QUERY_TYPE_INSERT:
										query = queue.insert;
										break;
									case MySQLQueue.QUERY_TYPE_DELETE:
										query = queue.delete;
										break;
									case MySQLQueue.QUERY_TYPE_UPDATE:
										query = queue.update;
										break;
									}
									addQueue(sql, params, query);
									queue.count++;
								}
								// lock end
								// update log position
								parser.setLogPos(beh.getNextPosition());
								parser.setLogTimestamp(event.getHeader().getTimestamp());
								queue.lastType = sqlType;
							}
						} catch (Exception e) {
							log.error("cause an exception when onEvents, reason is [{}]", e);
							throw new RuntimeException(e);
						}
					}
				}
			}
		});

		try {
			// parser start
			parser.start();
			// executor start
			chExecutor = new CHExecutor(name, target.tMySQLHandler);
			chExecutor.start();
			// mark running
			running = true;
		} catch (Exception e) {
			log.error("", e);
			parser.stop();
			chExecutor.stop();
			throw e;
		}
	}

	public void stop() throws Exception {
		parser.stop();
		chExecutor.stop();
	}

	public boolean isRunning() {
		return running;
	}
}
