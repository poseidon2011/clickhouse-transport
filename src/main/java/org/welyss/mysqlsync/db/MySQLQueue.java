package org.welyss.mysqlsync.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLQueue {
	public static final int QUERY_TYPE_INSERT = 0x1;
	public static final int QUERY_TYPE_DELETE = 0x2;
	public static final int QUERY_TYPE_UPDATE = 0x3;
	public byte lastType = -1;
	public long count = 0;
	public Map<String, List<Object[]>> insert;
	public Map<String, List<Object[]>> delete;
	public Map<String, List<Object[]>> update;
	public MySQLQueue() {
		insert = new HashMap<String, List<Object[]>>();
		delete = new HashMap<String, List<Object[]>>();
		update = new HashMap<String, List<Object[]>>();
	}

	public void clear() {
		insert = new HashMap<String, List<Object[]>>();
		delete = new HashMap<String, List<Object[]>>();
		update = new HashMap<String, List<Object[]>>();
		count = 0;
		lastType = -1;
	}
}
