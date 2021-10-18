package org.welyss.mysqlsync.transport;

public interface Executor {
	void execute(MySQLQueue query) throws Exception;
}
