package com.cloudant.se.concurrent;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.cloudant.se.db.loader.App;

public class StatusingThreadPoolExecutor extends ThreadPoolExecutor {
	protected static final Logger	log		= Logger.getLogger(App.class);	;
	protected static final Timer	timer	= new Timer();

	public StatusingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
		setupStatusTask();
	}

	public StatusingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
		setupStatusTask();
	}

	public StatusingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
			RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
		setupStatusTask();
	}

	public StatusingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		setupStatusTask();
	}

	@Override
	protected void terminated() {
		try {
			timer.cancel();
		} catch (Exception e) {
		}

		logStatus();
	}

	protected void logStatus() {
		log.info(String.format("STATUS - [ActiveT=%d][MinT=%d][CurrentT=%d][MaxT=%d][WaitingTasks=%d][CompletedTasks=%d][TotalTasks=%d]",
				getActiveCount(), getCorePoolSize(), getPoolSize(), getMaximumPoolSize(), getQueue().size(), getCompletedTaskCount(), getTaskCount()));
	}

	private void setupStatusTask() {
		// long interval = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
		long interval = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
		timer.schedule(new StatusTask(), interval, interval);
	}

	class StatusTask extends TimerTask {
		@Override
		public void run() {
			logStatus();
		}
	}
}
