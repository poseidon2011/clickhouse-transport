package org.welyss.mysqlsync;

import java.time.LocalDateTime;
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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.welyss.mysqlsync.transport.Handler;

public class CHExecutor implements Executor, Runnable {
	protected Source source;
	private MySQLQueue queues;
	private Handler handler;
	private CompletionService<Integer> queryExecutor;
	private Thread executor;
	private boolean running = false;
	private final Logger log = LoggerFactory.getLogger(getClass());

	public CHExecutor(Source source, Handler tCHHandler, AtomicInteger count) {
		queues = new MySQLQueue(count);
		this.source = source;
		this.handler = tCHHandler;
		queryExecutor = new ExecutorCompletionService<Integer>(new ThreadPoolExecutor(3, 6, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>()));
	}

	public void start() {
		running = true;
		if (executor == null || !executor.isAlive()) {
			executor = new Thread(this, source.name);
			executor.start();
		}
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
			if (queues.count.intValue() > 0) {
				long elapsed = System.currentTimeMillis();
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
						queryExecutor.take().get();
					}
				}
				Parser parser = source.parser;
				log.info("[{}-{}] sync successful, count: {}, takes {} milliseconds, logFile: {}, logPos: {}.", source.name, source.target.name, queues.count, System.currentTimeMillis() - elapsed, parser.getLogFile(), parser.getSavepoint());
				queues.clear();
				savepoint(source.parser.getSavepoint(), source.parser.getLogTimestamp(), source.id);
			}
		}
	}

	public void savepoint(long logPos, long logTimestamp, int id) throws Exception {
		savepoint(null, logPos, logTimestamp, id);
	}

	public void savepoint(String logFile, long logPos, long logTimestamp, int id) throws Exception {
		LocalDateTime now = LocalDateTime.now();
		if (logFile == null) {
			handler.update("ALTER TABLE `" + handler.getDatabase() + "`.`ch_syncdata_savepoints` UPDATE log_pos=?,log_timestamp=?,modify=? WHERE id=?", logPos, logTimestamp, now, id);
		} else {
			handler.update("ALTER TABLE `" + handler.getDatabase() + "`.`ch_syncdata_savepoints` UPDATE log_file=?,log_pos=?,log_timestamp=?,modify=? WHERE id=?", logFile, logPos, logTimestamp, now, id);
		}
	}

	public MySQLQueue getQueues() {
		return queues;
	}

	public void setQueues(MySQLQueue queues) {
		this.queues = queues;
	}

	class Task implements Callable<Integer> {
		private Map<String, List<List<Object>>> queue;

		public Task(Map<String, List<List<Object>>> queue) {
			this.queue = queue;
		}

		@Override
		public Integer call() throws Exception {
			return handler.executeInTransaction(queue);
		}
	}
}
