package com.six.dcsjob.scheduler;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.six.dcsjob.Job;
import com.six.dcsjob.JobSnapshot;
import com.six.dcsjob.JobSnapshotStatus;
import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.api.DcsNodeManager;
import com.six.dcsnodeManager.impl.ZkDcsNodeManager;

import com.six.dcsjob.cache.JobRunningCache;
import com.six.dcsjob.executor.ExecutorManager;

/**
 * @author liusong
 * @date 2017年8月15日
 * @email 359852326@qq.com
 */
public class SchedulerManagerImpl implements SchedulerManager {

	final static Logger log = LoggerFactory.getLogger(SchedulerManagerImpl.class);

	final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	public static final String JOB_NAME_KEY = "jobName";

	public static final String SCHEDULER_MANAGER_KEY = "scheduleManager";

	private DcsNodeManager dcsNodeManager;

	private QueryJob queryJob;

	private LinkedBlockingQueue<Job> pendingExecuteQueue = new LinkedBlockingQueue<>();

	private JobRunningCache jobRunningCache;

	private final static String schedulerGroup = "exCrawler";

	private Scheduler scheduler;

	private Thread doJobThread;

	private Interner<String> keyLock = Interners.<String>newWeakInterner();

	public SchedulerManagerImpl(String appName, String clusterName, Node currentNode, long keepliveInterval,
			String zkConnection, int nodeRpcServerThreads, int nodeRpcClientThreads, QueryJob queryJob) {
		dcsNodeManager = new ZkDcsNodeManager(appName, clusterName, currentNode, keepliveInterval, zkConnection,
				nodeRpcServerThreads, nodeRpcClientThreads);
		dcsNodeManager.start();
		this.queryJob = queryJob;
		initScheduler();
		initDoJobThread();
	}

	private void initDoJobThread() {
		doJobThread = new Thread(() -> {
			log.info("start Thread{loop-read-pendingExecuteQueue-thread}");
			Job job = null;
			while (true) {
				try {
					job = pendingExecuteQueue.take();
					if (null != job) {
						doExecute(job);
					}
				} catch (Exception e1) {
				}
			}
		}, "loop-read-pendingExecuteQueue-thread");
		doJobThread.setDaemon(true);
		doJobThread.start();
	}

	private void initScheduler() {
		Properties props = new Properties();
		props.put("org.quartz.scheduler.instanceName", "DefaultQuartzScheduler");
		props.put("org.quartz.scheduler.rmi.export", false);
		props.put("org.quartz.scheduler.rmi.proxy", false);
		props.put("org.quartz.scheduler.wrapJobExecutionInUserTransaction", false);
		props.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
		props.put("org.quartz.threadPool.threadCount", "2");
		props.put("org.quartz.threadPool.threadPriority", "5");
		props.put("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread", true);
		props.put("org.quartz.jobStore.misfireThreshold", "60000");
		try {
			StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory(props);
			scheduler = stdSchedulerFactory.getScheduler();
			scheduler.start();
		} catch (SchedulerException e) {
			log.error("start scheduler err");
			System.exit(1);
		}
	}

	/**
	 * 本地执行 由手动执行和定时触发 调用
	 * 
	 * @param job
	 */
	@Override
	public void execute(String jobName) {
		Job job = queryJob.getJob(jobName);
		if (null != job && !pendingExecuteQueue.contains(job)) {
			synchronized (keyLock.intern(jobName)) {
				pendingExecuteQueue.add(job);
				log.info("already submit job[" + jobName + "] to queue ");
			}
		} else {
			log.info("ready to execute job[" + jobName + "] is null");
		}
	}

	/**
	 * call工作节点执行任务
	 * 
	 * @param node
	 *            工作节点
	 * @param jobName
	 *            任务name
	 */
	private void doExecute(Job job) {
		// 判断任务是否在运行
		if (!jobRunningCache.isRunning(job.getName())) {
			log.info("master node execute job[" + job.getName() + "]");
			// TODO 这里计算可执行资源时，需要进行资源隔离，避免并发导致同时分配
			String designatedNodeName = job.getDesignatedNodeName();
			int needNodes = job.getNeedNodes();
			int needThreads = job.getThreads();
			List<Node> freeNodes = getFreeNodes(designatedNodeName, needNodes, needThreads);
			if (null != freeNodes && freeNodes.size() > 0) {
				doExecute(job, freeNodes);
				return;
			} else {
				log.error("there is no node to execute job[" + job.getName() + "]");
			}
		} else {
			log.error("the job[" + job.getName() + "] is running");
		}
	}

	/**
	 * 通知freeNodes 执行job
	 * 
	 * @param job
	 * @param freeNodes
	 */
	private void doExecute(Job job, List<Node> freeNodes) {
		String jobSnapshotId = "";
		JobSnapshot jobSnapshot = new JobSnapshot(jobSnapshotId, job.getName());
		// 任务开始时候 开始时间和结束时间默认是一样的
		jobSnapshot.setStartTime(DateFormatUtils.format(System.currentTimeMillis(), DATE_FORMAT));
		jobSnapshot.setEndTime(DateFormatUtils.format(System.currentTimeMillis(), DATE_FORMAT));
		jobSnapshot.setStatus(JobSnapshotStatus.INIT);
		jobRunningCache.register(jobSnapshot);
		ExecutorManager executorManager = null;
		for (Node freeNode : freeNodes) {
			try {
				executorManager = dcsNodeManager.loolupService(freeNode, ExecutorManager.class, response -> {
				});
				executorManager.execute(job, jobSnapshotId);
				log.info("already request worker node[" + freeNode.getName() + "] to execut the job[" + job.getName()
						+ "]");
			} catch (Exception e) {
				log.error("this master node calls worker node[" + freeNode.getName() + "] to execut the job["
						+ job.getName() + "]", e);
			}
		}
	}

	/**
	 * 获取可执行job的空闲节点
	 * 
	 * @param job
	 * @return
	 */
	private List<Node> getFreeNodes(String designatedNodeName, int needNodes, int needThreads) {
		List<Node> freeNodes = null;
		if (StringUtils.isNotBlank(designatedNodeName)) {
			Node designatedNode = dcsNodeManager.getSlaveNode(designatedNodeName);
			freeNodes = Arrays.asList(designatedNode);
		} else {
			freeNodes = dcsNodeManager.getNodes();
		}
		return freeNodes;
	}

	// private void doJobRelationship(JobSnapshot jobSnapshot, int executeType)
	// {
	// List<JobRelationship> jobRelationships =
	// getJobRelationshipDao().query(jobSnapshot.getName());
	// // TODO 这里并发触发的话，需要考虑 是否成功并发执行
	// for (JobRelationship jobRelationship : jobRelationships) {
	// if (executeType == jobRelationship.getExecuteType()) {
	// execute(TriggerType.newDispatchTypeByJob(jobSnapshot.getName(),
	// jobSnapshot.getId()),
	// jobRelationship.getNextJobName());
	// }
	// }
	// }

	@Override
	public void suspend(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			List<Node> nodes = jobRunningCache.runningJobNodes(jobName);
			ExecutorManager executorManager = null;
			for (Node node : nodes) {
				try {
					executorManager = dcsNodeManager.loolupService(node, ExecutorManager.class, result -> {
						// if (result.isOk() && isSuspend(jobName)) {
						// JobSnapshot jobSnapshot =
						// getScheduleCache().getJobSnapshot(jobName);
						// jobSnapshot.setStatus(JobSnapshotStatus.SUSPEND);
						// getScheduleCache().updateJobSnapshot(jobSnapshot);
						// }
					});
					executorManager.suspend(jobName);
					log.info("already request worker node[" + node.getName() + "] to suspend the job[" + jobName + "]");
				} catch (Exception e) {
					log.error("get node[" + node.getName() + "]'s workerSchedulerManager err", e);
				}
			}
		}
	}

	@Override
	public void goOn(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			List<Node> nodes = jobRunningCache.runningJobNodes(jobName);
			ExecutorManager executorManager = null;
			for (Node node : nodes) {
				try {
					executorManager = dcsNodeManager.loolupService(node, ExecutorManager.class, result -> {
						// if (result.isOk() && isRunning(jobName)) {
						// JobSnapshot jobSnapshot =
						// getScheduleCache().getJobSnapshot(jobName);
						// jobSnapshot.setStatus(JobSnapshotStatus.EXECUTING);
						// getScheduleCache().updateJobSnapshot(jobSnapshot);
						// }
					});
					executorManager.goOn(jobName);
					log.info("already request worker node[" + node.getName() + "] to goOn the job[" + jobName + "]");
				} catch (Exception e) {
					log.error("get node[" + node.getName() + "]'s workerSchedulerManager err", e);
				}
			}
		}
	}

	@Override
	public void stop(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			List<Node> nodes = jobRunningCache.runningJobNodes(jobName);
			ExecutorManager executorManager = null;
			for (Node node : nodes) {
				try {
					executorManager = dcsNodeManager.loolupService(node, ExecutorManager.class, result -> {
					});
					executorManager.stop(jobName);
					log.info("already request worker node[" + node.getName() + "] to stop the job[" + jobName + "]");
				} catch (Exception e) {
					log.error("get node[" + node.getName() + "]'s workerSchedulerManager err", e);
				}
			}
		}
	}

	@Override
	public void askEnd(String jobName) {

	}

	@Override
	public synchronized void stopAll() {
		List<Node> nodes = dcsNodeManager.getSlaveNodes();
		ExecutorManager executorManager = null;
		for (Node node : nodes) {
			try {
				executorManager = dcsNodeManager.loolupService(node, ExecutorManager.class, result -> {
				});
				executorManager.stopAll();
				log.info("already request all node[" + node.getName() + "] to stopAll");
			} catch (Exception e) {
				log.error("get node[" + node.getName() + "]'s workerSchedulerManager err", e);
			}
		}
	}

	public static class ScheduledJob implements org.quartz.Job {
		@Override
		public void execute(JobExecutionContext context) throws JobExecutionException {
			SchedulerManager scheduleManager = (SchedulerManager) context.getJobDetail().getJobDataMap()
					.get(SCHEDULER_MANAGER_KEY);
			String jobName = (String) context.getJobDetail().getJobDataMap().get(JOB_NAME_KEY);
			scheduleManager.execute(jobName);
		}
	}

	/**
	 * 向调度器注册job
	 * 
	 * @param job
	 */
	@Override
	public void schedule(Job job) {
		if (null != job) {
			synchronized (scheduler) {
				JobKey jobKey = new JobKey(job.getName(), schedulerGroup);
				try {
					boolean existed = scheduler.checkExists(jobKey);
					if (existed) {
						return;
					}
				} catch (SchedulerException e1) {
					log.error("scheduler checkExists{" + job.getName() + "} err", e1);
					return;
				}
				try {
					boolean existed = scheduler.checkExists(jobKey);
					if (existed) {
						return;
					}
				} catch (SchedulerException e1) {
					log.error("scheduler checkExists{" + job.getName() + "} err", e1);
					return;
				}
				if (StringUtils.isNotBlank(job.getCronTrigger())) {
					Trigger trigger = TriggerBuilder.newTrigger().withIdentity(job.getName(), schedulerGroup)
							.withSchedule(CronScheduleBuilder.cronSchedule(job.getCronTrigger())).startNow().build();
					JobBuilder jobBuilder = JobBuilder.newJob(ScheduledJob.class);
					jobBuilder.withIdentity(jobKey);
					JobDataMap newJobDataMap = new JobDataMap();
					newJobDataMap.put(JOB_NAME_KEY, job.getName());
					newJobDataMap.put(SCHEDULER_MANAGER_KEY, this);
					jobBuilder.setJobData(newJobDataMap);
					JobDetail jobDetail = jobBuilder.build();
					try {
						scheduler.scheduleJob(jobDetail, trigger);
					} catch (SchedulerException e) {
						log.error("scheduleJob err:" + job.getName());
					}

				}
			}
		}
	}

	@Override
	public void unschedule(String jobName) {
		if (StringUtils.isNotBlank(jobName)) {
			synchronized (scheduler) {
				try {
					JobKey key = new JobKey(jobName, schedulerGroup);
					scheduler.deleteJob(key);
				} catch (SchedulerException e) {
					log.error("deleteJobFromScheduled err", e);
				}
			}
		}
	}

	@Override
	public void shutdown() {
		if (null != scheduler) {
			try {
				scheduler.shutdown();
			} catch (SchedulerException e) {
				log.error("scheduler shutdown err");
			}
		}
		if (null != dcsNodeManager) {
			dcsNodeManager.shutdown();
		}
	}

}
