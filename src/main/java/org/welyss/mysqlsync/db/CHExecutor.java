package org.welyss.mysqlsync.db;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CHExecutor implements Executor {
	private MySQLHandler handler;
	private CompletionService<Integer> queryExecutor;

	public CHExecutor(MySQLHandler tMySQLHandler) {
		this.handler = tMySQLHandler;
		queryExecutor = new ExecutorCompletionService<Integer>(
				new ThreadPoolExecutor(3, 6, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>()));
	}

	@Override
	public void execute(MySQLQueue query) throws Exception {
		synchronized (query) {
			int taskCnt = 0;
			if (query.insert.size() > 0) {
				queryExecutor.submit(new Task(query.insert));
				taskCnt++;
			}
			if (query.delete.size() > 0) {
				queryExecutor.submit(new Task(query.delete));
				taskCnt++;
			}
			if (query.update.size() > 0) {
				queryExecutor.submit(new Task(query.update));
				taskCnt++;
			}
			for (int i = 0; i < taskCnt; i++) {
				queryExecutor.take();
			}
			query.clear();
		}
	}

	public void savepoint(long logPos, long logTimestamp, int id) throws Exception {
		savepoint(null, logPos, logTimestamp, id);
	}

	public void savepoint(String logFile, long logPos, long logTimestamp, int id) throws Exception {
		if (logFile == null) {
			handler.update("UPDATE ch_syncdata_savepoints SET log_pos=?,log_timestamp=? WHERE id=?", logPos, logTimestamp, id);
		} else {
			handler.update("UPDATE ch_syncdata_savepoints SET log_file=?,log_pos=?,log_timestamp=? WHERE id=?", logFile, logPos, logTimestamp, id);
		}
	}

	class Task implements Callable<Integer> {
		private Map<String,List<Object[]>> queue;
		public Task(Map<String,List<Object[]>> queue) {
			this.queue = queue;
		}

		@Override
		public Integer call() throws Exception {
			return handler.executeInTransaction(queue);
		}
	}
}
