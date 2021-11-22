package org.welyss.mysqlsync;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MySQLQueue {
	public static final int QUERY_TYPE_INSERT = 0x1;
	public static final int QUERY_TYPE_DELETE = 0x2;
	public static final int QUERY_TYPE_UPDATE = 0x3;
	protected Map<String, MySQLTableQueue> tableQueues = new HashMap<String, MySQLTableQueue>();
	protected AtomicInteger count;

	public MySQLQueue(AtomicInteger count) {
		this.count = count;
	}

	public void clear() {
		Iterator<MySQLTableQueue> queuesIt = tableQueues.values().iterator();
		while (queuesIt.hasNext()) {
			MySQLTableQueue queue = queuesIt.next();
			queue.clear();
		}
//		tableQueues.clear();
//		tableQueues = new HashMap<String, MySQLTableQueue>();
		count.set(0);
	}
}
