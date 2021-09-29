package org.welyss.mysqlsync.db;

import java.util.List;

public class MySQLTable {

	public String name;
	public List<MySQLColumn> columns;
	public String[] uniqueKey;

	public MySQLTable(String name, List<MySQLColumn> columns, String[] uniqueKey) {
		this.name = name;
		this.columns = columns;
		this.uniqueKey = uniqueKey;
	}

}
