package org.welyss.mysqlsync;

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
import org.welyss.mysqlsync.transport.Handler;

public class CHExecutor implements Executor, Runnable {
	protected Source source;
	private MySQLQueue queues = new MySQLQueue();
	private Handler handler;
	private CompletionService<Integer> queryExecutor;
	private Thread executor;
	private boolean running = false;
	private final Logger log = LoggerFactory.getLogger(getClass());

	public CHExecutor(Source source, Handler tMySQLHandler) {
		this.source = source;
		this.handler = tMySQLHandler;
		queryExecutor = new ExecutorCompletionService<Integer>(
				new ThreadPoolExecutor(3, 6, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>()));
	}

	public void start() {
		running = true;
		executor = new Thread(this, source.name);
		executor.start();
	}

	public void stop() {
		running = false;
	}

	@Override
	public void run() {
		while (running) {
			try {
				execute();
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				log.warn("cause exception when executor: [{}-{}] sleep, {}", source.name, e);
			} catch (Exception e) {
				log.warn("cause exception when executor: [{}-{}] execute, {}", source.name, e);
			}
		}
	}

	@Override
	public void execute() throws Exception {
		synchronized (queues) {
			Iterator<Entry<String, MySQLTableQueue>> queryIt = queues.tableQueues.entrySet().iterator();
			while (queryIt.hasNext()) {
				Entry<String, MySQLTableQueue> entry = queryIt.next();
				MySQLTableQueue tableQueue = entry.getValue();
				int taskCnt = 0;
				if (tableQueue.insert.size() > 0) {
					queryExecutor.submit(new Task(tableQueue.insert));
					taskCnt++;
				}
				if (tableQueue.delete.size() > 0) {
					queryExecutor.submit(new Task(tableQueue.delete));
					taskCnt++;
				}
				if (tableQueue.update.size() > 0) {
					queryExecutor.submit(new Task(tableQueue.update));
					taskCnt++;
				}
				for (int i = 0; i < taskCnt; i++) {
					queryExecutor.take();
				}
			}
			queues.clear();
			savepoint(source.parser.getLogPos(), source.parser.getLogTimestamp(), source.id);
		}
	}

	public void savepoint(long logPos, long logTimestamp, int id) throws Exception {
		savepoint(null, logPos, logTimestamp, id);
	}

	public void savepoint(String logFile, long logPos, long logTimestamp, int id) throws Exception {
		if (logFile == null) {
			handler.update("UPDATE ch_syncdata_savepoints SET log_pos=?,log_timestamp=? WHERE id=?", logPos,
					logTimestamp, id);
		} else {
			handler.update("UPDATE ch_syncdata_savepoints SET log_file=?,log_pos=?,log_timestamp=? WHERE id=?", logFile,
					logPos, logTimestamp, id);
		}
	}

	public MySQLQueue getQueues() {
		return queues;
	}

	public void setQueues(MySQLQueue queues) {
		this.queues = queues;
	}

	class Task implements Callable<Integer> {
		private Map<String, List<Object[]>> queue;

		public Task(Map<String, List<Object[]>> queue) {
			this.queue = queue;
		}

		@Override
		public Integer call() throws Exception {
			return handler.executeInTransaction(queue);
		}
	}
}
