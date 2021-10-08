package org.welyss.mysqlsync.db;

import java.util.ArrayList;
import java.util.List;

public class MySQLQuery {
	public static final int QUERY_TYPE_INSERT = 0x1;
	public static final int QUERY_TYPE_DELETE = 0x2;
	public static final int QUERY_TYPE_UPDATE = 0x3;
	public byte lastType = -1;
	public long count = 0;
	public List<String> insert;
	public List<String> delete;
	public List<String> update;
	public MySQLQuery() {
		insert = new ArrayList<String>();
		delete = new ArrayList<String>();
		update = new ArrayList<String>();
	}

	public void clear() {
		insert = new ArrayList<String>();
		delete = new ArrayList<String>();
		update = new ArrayList<String>();
		lastType = -1;
	}
}
