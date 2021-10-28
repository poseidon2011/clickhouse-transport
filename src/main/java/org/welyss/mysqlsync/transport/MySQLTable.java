package org.welyss.mysqlsync.transport;

import java.util.List;

public class MySQLTable {

	public String name;
	public List<MySQLColumn> columns;
	public Integer[] uniqueKey;

	public MySQLTable(String name, List<MySQLColumn> columns, Integer[] uniqueKey) {
		this.name = name;
		this.columns = columns;
		this.uniqueKey = uniqueKey;
	}

}
