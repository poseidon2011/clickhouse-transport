package org.welyss.mysqlsync;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLTableQueue {
	public byte lastType = -1;
	public long count = 0;
	public Map<String, List<Object[]>> insert;
	public Map<String, List<Object[]>> delete;
	public Map<String, List<Object[]>> update;
	public MySQLTableQueue() {
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
