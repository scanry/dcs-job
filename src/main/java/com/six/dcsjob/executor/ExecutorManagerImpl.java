package com.six.dcsjob.executor;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.six.dcsjob.work.AbstractWorkerPlugsManager;
import com.six.dcsjob.work.Worker;
import com.six.dcsnodeManager.DcsNodeManager;
import com.six.dcsjob.cache.JobRunningCache;
import com.six.dcsjob.model.Job;
import com.six.dcsjob.scheduler.SchedulerManager;

/**
 * @author liusong
 * @date 2017年8月15日
 * @email 359852326@qq.com
 */
public class ExecutorManagerImpl implements ExecutorManager {

	final static Logger log = LoggerFactory.getLogger(ExecutorManagerImpl.class);

	private static Interner<String> keyLock = Interners.<String>newWeakInterner();

	private DcsNodeManager dcsNodeManager;
	
	private AbstractWorkerPlugsManager workerPlugsManager;

	private JobRunningCache jobRunningCache;

	private WorkerParameterAssembling workerParameterAssembling;

	private ExecutorService executor;

	public ExecutorManagerImpl(DcsNodeManager dcsNodeManager,
			JobRunningCache jobRunningCache,
			int workSize, 
			WorkerParameterAssembling workerParameterAssembling) {
		this.dcsNodeManager=dcsNodeManager;
		this.jobRunningCache=jobRunningCache;
		executor = Executors.newFixedThreadPool(workSize);
		this.workerParameterAssembling = workerParameterAssembling;
	}

	/**
	 * 由内部守护线程循环读取等待被执行队列worker 执行此 调度执行job
	 * 
	 * 方法需要加分布式锁保证job的每个worker都是顺序被执行
	 * 
	 * @param job
	 */
	@Override
	public void execute(Job job, String jobSnapshotId) {
		if (null != job && StringUtils.isNotBlank(jobSnapshotId)) {
			synchronized (keyLock.intern(job.getName())) {
				if (null != job) {
					int needThreads = job.getThreads();
					int freeThreads = 2;
					int actualThreads = 0;
					if (needThreads <= freeThreads) {
						actualThreads = needThreads;
					} else {
						actualThreads = freeThreads;
					}
					Worker<?, ?>[] workers = workerPlugsManager.newWorker(job.getWorkerClass(), actualThreads);
					log.info("get " + actualThreads + " worker to execute job[" + job.getName() + "]");
					for (Worker<?, ?> worker : workers) {
						executor.execute(() -> {
							doExecute(job, jobSnapshotId, worker);
						});
						log.info("the job[" + job.getName() + "] is be executed by worker[" + worker.getName() + "]");
					}
				}
			}
		}
	}

	private void doExecute(final Job job, String jobSnapshotId, final Worker<?, ?> worker) {
		final String workerName = worker.getName();
		final Thread workerThread = Thread.currentThread();
		final String systemThreadName = workerThread.getName();
		final String newThreadName = systemThreadName + "-" + workerName;
		workerThread.setName(newThreadName);
		try {
			jobRunningCache.registerAndUpdate(job.getName(), jobSnapshotId, worker);
			workerParameterAssembling.setParameter(job);
			worker.start(job);
		} catch (Exception e) {
			log.error("execute worker [" + workerName + "] err", e);
		} finally {
			workerThread.setName(systemThreadName);
			worker.destroy();
			jobRunningCache.unregister(job.getName(), jobSnapshotId, worker);
		}
	}

	@Override
	public void suspend(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			Map<String, Worker<?, ?>> workers = jobRunningCache.getLocalWorkers(jobName);
			for (Worker<?, ?> worker : workers.values()) {
				worker.suspend();
			}
		}
	}

	@Override
	public void rest(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			Map<String, Worker<?, ?>> workers = jobRunningCache.getLocalWorkers(jobName);
			for (Worker<?, ?> worker : workers.values()) {
				worker.rest();
			}
		}
	}

	/**
	 * 工作节点被动继续任务
	 * 
	 * @param job
	 * @return
	 */
	@Override
	public void goOn(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			Map<String, Worker<?, ?>> workers = jobRunningCache.getLocalWorkers(jobName);
			for (Worker<?, ?> worker : workers.values()) {
				worker.goOn();
			}
		}
	}

	/**
	 * 工作节点被动停止任务
	 * 
	 * @param job
	 * @return
	 */
	@Override
	public void stop(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			Map<String, Worker<?, ?>> workers = jobRunningCache.getLocalWorkers(jobName);
			for (Worker<?, ?> worker : workers.values()) {
				worker.stop();
			}
		}
	}

	@Override
	public void finish(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			Map<String, Worker<?, ?>> workers = jobRunningCache.getLocalWorkers(jobName);
			for (Worker<?, ?> worker : workers.values()) {
				worker.finish();
			}
		}
	}

	@Override
	public void askEnd(String jobName, String workerName) {
		synchronized (keyLock.intern(jobName)) {
			SchedulerManager schedulerManager=dcsNodeManager.loolupService(dcsNodeManager.getMaster(), SchedulerManager.class);
			schedulerManager.askEnd(workerName);
		}
	}

	@Override
	public synchronized void stopAll() {
		for (Map<String, Worker<?, ?>> jobWorkerMap : jobRunningCache.getLocalAllWorkers().values()) {
			for (Worker<?, ?> worker : jobWorkerMap.values()) {
				worker.stop();
			}
		}
	}

	@Override
	public boolean isShutdown() {
		return jobRunningCache.localWorkersIsEnd();
	}

	@Override
	public void shutdown() {
		// 然后获取当前节点有关的job worker 然后调用stop
		stopAll();
		// 然后shut down worker线程池
		executor.shutdown();
	}

}
