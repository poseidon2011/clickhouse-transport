package org.welyss.mysqlsync.transport;

import java.lang.ref.SoftReference;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TableMetaCache {
	private static Map<String, SoftReference<MySQLTable>> cache = new HashMap<String, SoftReference<MySQLTable>>();
	@Autowired
	private DataSourceFactory cHDataSourceFactory;

	public MySQLTable get(String dbnm, String tablenm) throws Exception {
		MySQLTable table;
		String uniname = dbnm + '.' + tablenm;
		SoftReference<MySQLTable> tableRef = cache.get(uniname);
		if (tableRef == null || (table = tableRef.get()) == null) {
			// generate table meta info from mysql
			MySQLHandler handler = new MySQLHandler(dbnm, cHDataSourceFactory.takeMysql(dbnm));
			try {
				// | Field       | Type         | Null | Key | Default | Extra |
				List<Map<String, Object>> meta = handler.queryForMaps("DESC `" + tablenm + "`");
				List<MySQLColumn> columns = new ArrayList<MySQLColumn>(meta.size());
				List<Integer> uniqueKey = null;
				for (int j = 0; j < meta.size(); j++) {
					Map<String, Object> row = meta.get(j);
					String field = row.get("Field").toString();
					String key = (String) row.get("Key");
					MySQLColumn column = new MySQLColumn(j, field, "PRI".equals(key), row.get("Type").toString(), row.get("Null").toString().toUpperCase().equals("YES"));
					columns.add(column);
				}
				// | Table       | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
				List<Map<String, Object>> uniques = handler.queryForMaps("SHOW INDEX IN `" + tablenm + "` WHERE Non_unique=0");
				if (uniques.size() > 0) {
					uniqueKey = new ArrayList<Integer>(4);
					int oldSeqInIndex = 0;
					for (int i = 0; i < uniques.size(); i++) {
						Map<String, Object> item = uniques.get(i);
						int seqInIndex = Integer.parseInt(item.get("Seq_in_index").toString());
						if (seqInIndex <= oldSeqInIndex) break;
						for (int j = 0; j < columns.size(); j++) {
							if (item.get("Column_name").toString().equals(columns.get(j).name)) {
								uniqueKey.add(j);
								break;
							}
						}
						oldSeqInIndex = seqInIndex;
					}
				}
				table = new MySQLTable(tablenm, columns, uniqueKey.toArray(new Integer[uniqueKey.size()]));
			} catch (SQLException e) {
				throw new RuntimeException("get Table Meta faild.", e);
			}
			cache.put(uniname, new SoftReference<MySQLTable>(table));
		}
		return table;
	}

}
