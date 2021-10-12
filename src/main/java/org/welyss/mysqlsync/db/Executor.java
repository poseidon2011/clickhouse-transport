package org.welyss.mysqlsync.db;

public interface Executor {
	void execute(MySQLQueue query) throws Exception;
}
