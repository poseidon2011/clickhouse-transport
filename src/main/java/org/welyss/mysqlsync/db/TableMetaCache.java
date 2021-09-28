package org.welyss.mysqlsync.db;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

public class TableMetaCache {
	private static Map<String, SoftReference<Table>> cache;
	public TableMetaCache() {
		cache = new HashMap<String, SoftReference<Table>>();
	}

	public static void put(String key, Table table) {
		cache.put(key, new SoftReference<Table>(table));
	}

	public static Table get(String schema, String tablenm) {
		Table table;
		String key = schema + '.' + tablenm;
		SoftReference<Table> tableRef = cache.get(key);
		if (tableRef == null || (table = tableRef.get()) == null) {
			// 
			table = new Table(tablenm);
		}
		return table;
	}

}
