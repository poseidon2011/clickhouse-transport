package org.welyss.mysqlsync.transport;

import java.util.List;

public class MySQLTable {

	public String name;
	public List<MySQLColumn> columns;
	public int[] uniqueKey;

	public MySQLTable(String name, List<MySQLColumn> columns, int[] uniqueKey) {
		this.name = name;
		this.columns = columns;
		this.uniqueKey = uniqueKey;
	}

}
