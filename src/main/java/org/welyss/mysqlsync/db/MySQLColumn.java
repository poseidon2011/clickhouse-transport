package org.welyss.mysqlsync.db;

public class MySQLColumn {
	public int order;
	public String name;
	public String type;
	public boolean pk;
	public boolean nullable;

	public MySQLColumn(int order, String name, boolean pk, String type, boolean nullable) {
		this.order = order;
		this.name = name;
		this.type = type;
		this.pk = pk;
		this.nullable = nullable;
	}
}
