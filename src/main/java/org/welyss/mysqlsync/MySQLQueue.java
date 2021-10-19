package org.welyss.mysqlsync;

import java.util.HashMap;
import java.util.Map;

public class MySQLQueue {
	public static final int QUERY_TYPE_INSERT = 0x1;
	public static final int QUERY_TYPE_DELETE = 0x2;
	public static final int QUERY_TYPE_UPDATE = 0x3;
	protected Map<String, MySQLTableQueue> tableQueues = new HashMap<String, MySQLTableQueue>();
	protected int count;

	public void clear() {
		tableQueues = new HashMap<String, MySQLTableQueue>();
		count = 0;
	}
}
