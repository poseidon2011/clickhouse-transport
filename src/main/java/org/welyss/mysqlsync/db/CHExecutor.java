package org.welyss.mysqlsync.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.welyss.mysqlsync.CommonUtils;

public class CHExecutor implements Executor, Runnable {
	protected String name;
	private Map<String, MySQLQueue> querys = new HashMap<String, MySQLQueue>();
	private CHHandler handler;
	private CompletionService<Integer> queryExecutor;
	private Thread executor;
	private boolean running = false;
	private final Logger log = LoggerFactory.getLogger(getClass());

	public CHExecutor(String name, CHHandler tMySQLHandler) {
		this.name = name;
		this.handler = tMySQLHandler;
		queryExecutor = new ExecutorCompletionService<Integer>(
				new ThreadPoolExecutor(3, 6, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>()));
	}

	public void start() {
		running = true;
		executor = new Thread(this, name);
		executor.start();
	}

	public void stop() {
		running = false;
	}

	@Override
	public void run() {
		while(running) {
			Iterator<Entry<String, MySQLQueue>> queryIt = querys.entrySet().iterator();
			while (queryIt.hasNext()) {
				Entry<String, MySQLQueue> entry = queryIt.next();
				String table = entry.getKey();
				MySQLQueue queue = entry.getValue();
				if (queue.count >= CommonUtils.bufferSize) {
					while(true) {
						try {
							execute(queue);
						} catch (Exception e) {
							log.error("cause exception when executor: [{}-{}] execute, {}", name, table, e);
						}
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							log.warn("cause exception when executor: [{}] sleep, {}", name, e);
						}
					}
				}
			}

			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				log.warn("cause exception when executor: [{}-{}] sleep, {}", name, e);
			}
		}
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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, MySQLQueue> getQuerys() {
		return querys;
	}

	public void setQuerys(Map<String, MySQLQueue> querys) {
		this.querys = querys;
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
