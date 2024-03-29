package org.welyss.mysqlsync;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.welyss.mysqlsync.transport.DataSourceFactory;
import org.welyss.mysqlsync.transport.DataSourceFactory.HostInfo;
import org.welyss.mysqlsync.transport.MySQLColumn;
import org.welyss.mysqlsync.transport.MySQLTable;
import org.welyss.mysqlsync.transport.TableMetaCache;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserListener;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;

import io.micrometer.core.instrument.MeterRegistry;

@Component
public class Source {
	private static final String DEFAULT_MYSQL_PORT = "3306";
	protected Integer id;
	protected String name;
	protected Target target;
	protected Set<String> syncTables = new HashSet<String>();
	protected Parser parser;
	private final Logger log = LoggerFactory.getLogger(getClass());
	private CHExecutor chExecutor;
	private boolean running = false;
	@Value("${base.server.id}")
	private int baseServerId;

	@Autowired
	private MeterRegistry registry;
	@Autowired
	private DataSourceFactory dataSourceFactory;
	@Autowired
	private TableMetaCache tableMetaCache;

	private void addQueue(String sql, List<List<Object>> params, Map<String, List<List<Object>>> queue) {
		if (queue.containsKey(sql)) {
			queue.get(sql).addAll(params);
		} else {
			queue.put(sql, params);
		}
	}

	private Object formatVal(MySQLTable table, MySQLColumn column, Object val) {
		Object result = null;
		if (val != null) {
			if (column.type.equals("timestamp") || column.type.equals("datetime")) {
				if (val.getClass().isAssignableFrom(Date.class)) {
					result = LocalDateTime.ofInstant(((Date)val).toInstant(), ZoneId.systemDefault());
				} else {
					try {
						result = CommonUtils.parseDateShort(val.toString());
					} catch (DateTimeParseException e) {
						try {
							result = CommonUtils.parseDate(val.toString());
						} catch (DateTimeParseException dtpe) {
							log.warn("from is [{}], to is [{}], table is [{}], columnInfo.name is [{}], columnInfo.type is [{}], val of class is [{}], val is [{}], {}",
									name, target.name, table.name, column.name, column.type,
									val.getClass().getName(), val, e);
							result = val;
						}
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
			} else if (val instanceof byte[]) {
				result = new String((byte[])val);
			} else {
				result = val;
			}
		} else {
			if (column.type.startsWith("char")
					|| column.type.startsWith("varchar")
					|| column.type.startsWith("binary")
					|| column.type.startsWith("varbinary")
					|| column.type.startsWith("blob")
					|| column.type.startsWith("text")
					|| column.type.startsWith("enum")
					|| column.type.startsWith("set")) {
				result = "";
			} else {
				result = val;
			}
		}
		return result;
	}

	private BigDecimal readUnsignedLong(long value) {
		if (value >= 0)
			return new BigDecimal(value);
		long lowValue = value & Long.MAX_VALUE;
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
		if (parser == null) {
			HostInfo hostInfo = dataSourceFactory.getHostInfo(name);
			AtomicLong gaugeLogPos = registry.gauge("clickhouse_transport_logpos", new AtomicLong(logPos));
			AtomicLong gaugeLogTimestamp = registry.gauge("clickhouse_transport_logtimestamp", new AtomicLong(logTimestamp));
			parser = new MyBinlogParser(baseServerId + id, name + "-" + target.name, hostInfo.host, hostInfo.port == null ? DEFAULT_MYSQL_PORT : hostInfo.port, hostInfo.username, hostInfo.password, logFile, logPos, gaugeLogPos, gaugeLogTimestamp);
			parser.setBinlogEventListener(new BinlogEventListener() {
				@Override
				public void onEvents(BinlogEventV4 event) {
//					long elapsed = System.currentTimeMillis();
					try {
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
									List<List<Object>> params = new ArrayList<List<Object>>();
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
											Pair<Row> pair = rows.get(0);
											boolean changed = false;
											StringBuilder whereCause = new StringBuilder();
											StringBuilder setCause = new StringBuilder();
											List<Object> whereParams = new ArrayList<Object>();
											List<Column> before = pair.getBefore().getColumns();
											List<Column> after = pair.getAfter().getColumns();
											Object[] formattedBefVals = new Object[before.size()];
											List<Object> param = new ArrayList<Object>();
											for (int j = 0; j < columnLen; j++) {
												MySQLColumn column = table.columns.get(j);
												Object beforeVal = formatVal(table, column, before.get(j).getValue());
												formattedBefVals[j] = beforeVal;
												Object afterVal = formatVal(table, column, after.get(j).getValue());
												if (!equalObj(beforeVal, afterVal)) {
													setCause.append("`").append(column.name).append("`=?,");
													param.add(afterVal);
													changed = true;
												}
											}
											if (table.uniqueKey.length > 0) {
												for (int j = 0; j < table.uniqueKey.length; j++) {
													int uniqIndex = table.uniqueKey[j];
													whereCause.append('`').append(table.columns.get(uniqIndex).name).append("`=? and ");
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
											param.addAll(whereParams);
											params.add(param);
											for (int i = 1; i < rows.size(); i++) {
												pair = rows.get(i);
												whereParams = new ArrayList<Object>();
												before = pair.getBefore().getColumns();
												after = pair.getAfter().getColumns();
												param = new ArrayList<Object>();
												for (int j = 0; j < columnLen; j++) {
													MySQLColumn column = table.columns.get(j);
													Object beforeVal = formatVal(table, column, before.get(j).getValue());
													Object afterVal = formatVal(table, column, after.get(j).getValue());
													if (!equalObj(beforeVal, afterVal)) {
														param.add(afterVal);
														changed = true;
													}
												}
												param.addAll(whereParams);
												params.add(param);
											}
											if (changed) {
												sql = "ALTER TABLE `" + target.tCHHandler.getDatabase() + "`.`" + table.name + "` UPDATE " + setCause + "WHERE " + whereCause;
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
											StringBuilder selColumns = new StringBuilder();
											StringBuilder sqlVals = new StringBuilder();
											Row row = rows.get(0);
											List<Column> columns = row.getColumns();
											List<Object> param = new ArrayList<Object>();
											try {
												for (int j=0; j<columnLen; j++) {
													MySQLColumn column = table.columns.get(j);
													sqlVals.append("?,");
													selColumns.append("`").append(column.name).append("`,");
													param.add(formatVal(table, column, columns.get(j).getValue()));
												}
												selColumns.deleteCharAt(selColumns.length() - 1);
												sqlVals.deleteCharAt(sqlVals.length() - 1);
												params.add(param);
												for (int i = 1; i < rows.size(); i++) {
													row = rows.get(i);
													columns = row.getColumns();
													param = new ArrayList<Object>();
													for (int j=0; j<columnLen; j++) {
														MySQLColumn column = table.columns.get(j);
														param.add(formatVal(table, column, columns.get(j).getValue()));
													}
													params.add(param);
												}
												sql = "INSERT INTO `" + table.name + "`(" + selColumns + ")VALUES(" + sqlVals + ")";
											} catch (IndexOutOfBoundsException ioobe) {
												Iterator<MySQLColumn> itci = table.columns.iterator();
												StringBuilder sb = new StringBuilder();
												while (itci.hasNext()) {
													sb.append(itci.next().name).append(",");
												}
												log.error("target table:[{}.{}] tableInfo.columns: {}=>[{}] compare with binlog.columns size: {}."
														, target.tCHHandler.getSchema(), table.name, table.columns.size(), sb, columnLen);
												throw ioobe;
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
											Row row = rows.get(0);
											StringBuilder whereCause = new StringBuilder();
											List<Column> columns = row.getColumns();
											List<Object> param = new ArrayList<Object>();
											for (int j=0; j<table.uniqueKey.length; j++) {
												MySQLColumn column = table.columns.get(table.uniqueKey[j]);
												if (j > 0) {
													whereCause.append(" and ");
												}
												whereCause.append('`').append(column.name).append("`=?");
												param.add(formatVal(table, column, columns.get(column.order).getValue()));
											}
											params.add(param);
											for (int i = 1; i < rows.size(); i++) {
												row = rows.get(i);
												columns = row.getColumns();
												param = new ArrayList<Object>();
												for (int j=0; j<table.uniqueKey.length; j++) {
													MySQLColumn column = table.columns.get(table.uniqueKey[j]);
													param.add(formatVal(table, column, columns.get(column.order).getValue()));
												}
												params.add(param);
											}
											sql = "ALTER TABLE `" + target.tCHHandler.getDatabase() + "`.`" + table.name + "` DELETE WHERE " + whereCause;
										}
									}
									// get query and ready to execute
									MySQLQueue queues = chExecutor.getQueues();
									// exec
									if (sql != null) {
										// lock start
										synchronized (queues) {
											Map<String, MySQLTableQueue> tableQueues = queues.tableQueues;
											MySQLTableQueue tableQueue;
											if (tableQueues.containsKey(table.name)) {
												tableQueue = tableQueues.get(table.name);
											} else {
												tableQueue = new MySQLTableQueue();
												tableQueues.put(table.name, tableQueue);
											}
											if (confict(tableQueue.lastType, sqlType)) {
												try {
													log.debug("[{}-{}: {}] confict, executing, taskQuene.total is [{}].", name, target.name, table.name, tableQueue.count);
													chExecutor.execute();
												} catch (Exception e) {
													log.error("cause an exception when chExecutor.exec, reason is [{}]", e.getMessage());
													throw new RuntimeException(e);
												}
											} else if (tableQueue.count >= CommonUtils.bufferSize) {
												try {
													log.debug("[{}-{}: {}] buffer full, executing, taskQuene.total is [{}].", name, target.name, table.name, tableQueue.count);
													chExecutor.execute();
												} catch (Exception e) {
													log.error("cause an exception when chExecutor.exec, reason is [{}]", e.getMessage());
													throw new RuntimeException(e);
												}
											}
											log.debug("task[{}-{}-{}] in source lock.", name, target.name, table.name);

											// add task
											Map<String, List<List<Object>>> query = null;
											switch (sqlType) {
											case MySQLQueue.QUERY_TYPE_INSERT:
												query = tableQueue.insert;
												break;
											case MySQLQueue.QUERY_TYPE_DELETE:
												query = tableQueue.delete;
												break;
											case MySQLQueue.QUERY_TYPE_UPDATE:
												query = tableQueue.update;
												break;
											}
											addQueue(sql, params, query);
											tableQueue.count += params.size();
											queues.count.set(queues.count.intValue() + params.size());

											// update log position
											parser.setLogPos(beh.getNextPosition());
											parser.setLogTimestamp(event.getHeader().getTimestamp());
											tableQueue.lastType = sqlType;
										}
										// lock end
									}
								} catch (Exception e) {
									log.error("cause an exception when onEvents, reason is [{}]", e.getMessage());
									throw new RuntimeException(e);
								}
							}
						} else if (MySQLConstants.ROTATE_EVENT == type) {
							final RotateEvent re = (RotateEvent)event;
							MySQLQueue queues = chExecutor.getQueues();
							synchronized (queues) {
								boolean done = false;
								while (!done) {
									try {
										String logFile = re.getBinlogFileName().toString();
										long logPos = re.getBinlogPosition();
										long logTimestamp = re.getHeader().getTimestamp();
										parser.setLogFile(logFile);
										parser.setLogPos(logPos);
										parser.setSavepoint(logPos);
										parser.setLogTimestamp(logTimestamp);
										chExecutor.execute();
										chExecutor.savepoint(logFile, logPos, logTimestamp, id);
										done = true;
									} catch (Exception e) {
										log.warn("cause an exception when rotate execute/savepoint, reason: {}", e.getMessage());
									}
								}
							}
						} else if (MySQLConstants.XID_EVENT == type) {
							parser.setSavepoint(beh.getNextPosition());
						}
						parser.setLogPos(beh.getNextPosition());
						parser.setLogTimestamp(event.getHeader().getTimestamp());
					} catch(Exception e) {
						log.error("cause exception when parser/chExecutor start, msg: {}", e);
						try {
							stop();
						} catch (Exception pe) {
							log.error("cause exception when stop(), msg: {}", e);
						}
						throw e;
					}
				}
			});
		}

		if (chExecutor == null) {
			AtomicInteger queueCount = registry.gauge("clickhouse_transport_queue_count", new AtomicInteger(0));
			chExecutor = new CHExecutor(this, target.tCHHandler, queueCount);
		}

		if (!running) {
			try {
				// parser start
				parser.setLogFile(logFile);
				parser.setLogPos(logPos);
				parser.setSavepoint(logPos);
				parser.setLogTimestamp(logTimestamp);
				parser.start();
				boolean myBinlogParserListenerExists = false;
				for (BinlogParserListener listener : parser.getParserListeners()) {
					if (listener instanceof MyBinlogParserListener) {
						myBinlogParserListenerExists = true;
						break;
					}
				}
				if (!myBinlogParserListenerExists) {
					parser.addParserListener(new MyBinlogParserListener());
				}

				// executor start
				chExecutor.start();
				// mark running
				running = true;
				log.info("====> Source Worker: {}-{} Start <====", name, target.name);
			} catch (Exception e) {
				log.error("cause exception when parser/chExecutor start, msg: {}", e);
				stop();
				throw e;
			}
		}
	}

	public void stop() throws Exception {
		parser.stop();
		chExecutor.stop();
		running = false;
	}

	public boolean isRunning() {
		return running;
	}

	class MyBinlogParserListener implements BinlogParserListener {
		@Override
		public void onException(BinlogParser parser, Exception exception) {
			log.error("cause exception when parser onException event, msg: {}", exception);
			try {
				stop();
			} catch (Exception pe) {
				log.error("cause exception when stop(), msg: {}", pe);
			}
		}
		@Override
		public void onStart(BinlogParser parser) {}
		@Override
		public void onStop(BinlogParser parser) {}
	}
}
