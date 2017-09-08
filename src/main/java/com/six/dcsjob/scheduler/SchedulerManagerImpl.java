package com.six.dcsjob.scheduler;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.ignite.cluster.ClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.six.dcsnodeManager.DcsNodeManager;
import com.six.dcsjob.cache.JobRunningCache;
import com.six.dcsjob.executor.ExecutorManager;
import com.six.dcsjob.model.Job;
import com.six.dcsjob.model.JobRelationship;
import com.six.dcsjob.model.JobRunningContext;
import com.six.dcsjob.model.JobSnapshot;
import com.six.dcsjob.model.JobSnapshotStatus;

/**
 * @author liusong
 * @date 2017年8月15日
 * @email 359852326@qq.com
 */
public class SchedulerManagerImpl implements SchedulerManager {

	final static Logger log = LoggerFactory.getLogger(SchedulerManagerImpl.class);

	final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	final static String JOBSNAPSHOTID_DATE_FORMAT = "yyyyMMddHHmmss";

	private DcsNodeManager dcsNodeManager;

	private JobQuery jobQuery;

	private JobRelationshipQuery jobRelationshipQuery;

	private LinkedBlockingQueue<JobRunningContext> pendingExecuteQueue = new LinkedBlockingQueue<>();

	private JobRunningCache jobRunningCache;

	private JobSnapshotReport jobSnapshotReport;

	private Thread doJobThread;

	private Interner<String> keyLock = Interners.<String>newWeakInterner();

	public SchedulerManagerImpl(DcsNodeManager dcsNodeManager, JobRunningCache jobRunningCache, JobQuery jobQuery,
			JobRelationshipQuery jobRelationshipQuery, JobSnapshotReport jobSnapshotReport) {
		this.dcsNodeManager = dcsNodeManager;
		this.jobRunningCache = jobRunningCache;
		this.jobQuery = jobQuery;
		this.jobRelationshipQuery = jobRelationshipQuery;
		this.jobSnapshotReport = jobSnapshotReport;
		initDoJobThread();
	}

	private void initDoJobThread() {
		doJobThread = new Thread(() -> {
			log.info("start Thread{loop-read-pendingExecuteQueue-thread}");
			JobRunningContext jobRunningContext = null;
			while (true) {
				try {
					jobRunningContext = pendingExecuteQueue.take();
					if (null != jobRunningContext) {
						doExecute(jobRunningContext);
					}
				} catch (Exception e) {
					log.error("", e);
				}
			}
		}, "loop-read-pendingExecuteQueue-thread");
		doJobThread.setDaemon(true);
		doJobThread.start();
	}

	/**
	 * 本地执行 由手动执行和定时触发 调用 任务开始时候 开始时间和结束时间默认是一样的
	 * 
	 * @param job
	 */
	@Override
	public void execute(String triggerJobName, String jobName) {
		if (StringUtils.isNotBlank(jobName)) {
			Job job = jobQuery.get(jobName);
			if (null != job) {
				synchronized (keyLock.intern(jobName)) {
					String jobSnapshotId = jobName + "_"
							+ DateFormatUtils.format(System.currentTimeMillis(), JOBSNAPSHOTID_DATE_FORMAT);
					JobSnapshot jobSnapshot = new JobSnapshot();
					jobSnapshot.setId(jobSnapshotId);
					jobSnapshot.setName(jobName);
					jobSnapshot.setStartTime(DateFormatUtils.format(System.currentTimeMillis(), DATE_FORMAT));
					jobSnapshot.setEndTime(DateFormatUtils.format(System.currentTimeMillis(), DATE_FORMAT));
					jobSnapshot.setStatus(JobSnapshotStatus.PENDING_EXECUTED);
					JobRunningContext jobRunningContext = new JobRunningContext();
					jobRunningContext.setJobSnapshot(jobSnapshot);
					jobRunningContext.setJob(job);
					pendingExecuteQueue.add(jobRunningContext);
					jobRunningCache.registerAndUpdate(jobSnapshot);
					log.info("th job[" + jobName + "] is be trigger by " + triggerJobName
							+ " and already submit to queue");
				}
			}
		} else {
			log.info("ready to execute job[" + jobName + "] is null");
		}
	}

	/**
	 * 处理任务执行
	 * 
	 * @param job
	 */
	private void doExecute(JobRunningContext jobRunningContext) {
		if (!jobRunningCache.isRunning(jobRunningContext.getJob().getName())) {
			synchronized (keyLock.intern(jobRunningContext.getJob().getName())) {
				if (!jobRunningCache.isRunning(jobRunningContext.getJob().getName())) {
					// TODO 这里计算可执行资源时，需要进行资源隔离，避免并发导致同时分配
					String designatedNodeName = jobRunningContext.getJob().getDesignatedNodeName();
					List<ClusterNode> freeNodes = null;
					if (StringUtils.isNotBlank(designatedNodeName)) {
						ClusterNode designatedNode = dcsNodeManager.getNode(designatedNodeName);
						freeNodes = Arrays.asList(designatedNode);
					} else {
						freeNodes = dcsNodeManager.getNodes();
					}
					if (null != freeNodes && freeNodes.size() > 0) {
						doExecute(jobRunningContext.getJob(), jobRunningContext.getJobSnapshot(), freeNodes);
						return;
					} else {
						log.info("there is no node to execute job[" + jobRunningContext.getJob().getName() + "]");
					}
				}
			}
		}
		log.error("the job[" + jobRunningContext.getJob().getName() + "] is running");
	}

	/**
	 * 任务调度器调度从节点执行任务 1.生成任务运行快照。 2.注册任务运行快照。 3.监听执行任务worker的启动事件。
	 * 4.监听执行任务worker的结束事件。 5.rpc调用指定从节点执行任务
	 * 
	 * @param job
	 *            执行的任务
	 * @param freeNodes
	 *            空闲节点
	 */
	private void doExecute(Job job, JobSnapshot jobSnapshot, List<ClusterNode> freeNodes) {
		jobSnapshot.setStatus(JobSnapshotStatus.INIT);
		jobRunningCache.registerAndUpdate(jobSnapshot);
		jobRunningCache.listenWorkerStart(jobSnapshot, workerSnapshot -> {
			doJobRelationship(jobSnapshot, JobRelationship.EXECUTE_TYPE_PARALLEL);
			// TODO 这里可以监听执行任务的worker启动事件
		});
		jobRunningCache.listenWorkerEnd(jobSnapshot, workerSnapshot -> {
			endjob(jobSnapshot);
			if (JobSnapshotStatus.FINISHED == jobSnapshot.getStatus()) {
				doJobRelationship(jobSnapshot, JobRelationship.EXECUTE_TYPE_SERIAL);
			}
		});
		for (ClusterNode freeNode : freeNodes) {
			try {
				dcsNodeManager.loolupService(freeNode, ExecutorManager.class, null).execute(job, jobSnapshot.getId());
				log.info("already request worker node[" + freeNode.id() + "] to execut the job[" + job.getName() + "]");
			} catch (Exception e) {
				log.error("this master node calls worker node[" + freeNode.id() + "] to execut the job["
						+ jobSnapshot.getName() + "]", e);
			}
		}
	}

	private void endjob(JobSnapshot jobSnapshot) {
		jobSnapshot.setEndTime(DateFormatUtils.format(new Date(), DATE_FORMAT));
		jobSnapshotReport.report(jobSnapshot);
		jobRunningCache.unregister(jobSnapshot);
	}

	private void doJobRelationship(JobSnapshot jobSnapshot, int executeType) {
		List<JobRelationship> jobRelationships = jobRelationshipQuery.get(jobSnapshot.getName());
		// TODO 这里并发触发的话，需要考虑 是否成功并发执行
		for (JobRelationship jobRelationship : jobRelationships) {
			if (executeType == jobRelationship.getExecuteType()) {
				execute(jobSnapshot.getName(), jobRelationship.getNextJobName());
			}
		}
	}

	@Override
	public void suspend(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			List<ClusterNode> nodes = jobRunningCache.runningJobNodes(jobName);
			ExecutorManager executorManager = null;
			for (ClusterNode node : nodes) {
				try {
					executorManager = dcsNodeManager.loolupService(node, ExecutorManager.class);
					executorManager.suspend(jobName);
					log.info("already request worker node[" + node.id() + "] to suspend the job[" + jobName + "]");
				} catch (Exception e) {
					log.error("get node[" + node.id() + "]'s workerSchedulerManager err", e);
				}
			}
		}
	}

	@Override
	public void goOn(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			List<ClusterNode> nodes = jobRunningCache.runningJobNodes(jobName);
			ExecutorManager executorManager = null;
			for (ClusterNode node : nodes) {
				try {
					executorManager = dcsNodeManager.loolupService(node, ExecutorManager.class);
					executorManager.goOn(jobName);
					log.info("already request worker node[" + node.id() + "] to goOn the job[" + jobName + "]");
				} catch (Exception e) {
					log.error("get node[" + node.id() + "]'s workerSchedulerManager err", e);
				}
			}
		}
	}

	@Override
	public void stop(String jobName) {
		synchronized (keyLock.intern(jobName)) {
			List<ClusterNode> nodes = jobRunningCache.runningJobNodes(jobName);
			ExecutorManager executorManager = null;
			for (ClusterNode node : nodes) {
				try {
					executorManager = dcsNodeManager.loolupService(node, ExecutorManager.class);
					executorManager.stop(jobName);
					log.info("already request worker node[" + node.id() + "] to stop the job[" + jobName + "]");
				} catch (Exception e) {
					log.error("get node[" + node.id() + "]'s workerSchedulerManager err", e);
				}
			}
		}
	}

	@Override
	public void askEnd(String jobName) {

	}

	@Override
	public synchronized void stopAll() {
		List<ClusterNode> nodes = dcsNodeManager.getSlaveNodes();
		ExecutorManager executorManager = null;
		for (ClusterNode node : nodes) {
			try {
				executorManager = dcsNodeManager.loolupService(node, ExecutorManager.class);
				executorManager.stopAll();
				log.info("already request all node[" + node.id() + "] to stopAll");
			} catch (Exception e) {
				log.error("get node[" + node.id() + "]'s workerSchedulerManager err", e);
			}
		}
	}

	@Override
	public void shutdown() {
	}

	@Override
	public JobRunningCache getJobRunningCache() {
		return jobRunningCache;
	}

}
