package org.welyss.mysqlsync.db;

import java.util.List;

public class Table {

	String name;
	List<Column> columns;

	public Table(String name) {
		this.name = name;
	}

}
