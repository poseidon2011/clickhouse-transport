package org.welyss.mysqlsync.db;

public class Column {
	int order;
	String name;
	String type;
	boolean isPk;
	String isSensitive;
	String sensitiveRule;
	boolean isNull;
	boolean want = true;

	public Column(int order, String name, boolean isPk, String type, boolean isNull) {
		this.order = order;
		this.name = name;
		this.isPk = isPk;
		this.type = type;
		this.isNull = isNull;
	}

	public void setSensitive(String isSensitive) {
		this.isSensitive = isSensitive;
	}
}
