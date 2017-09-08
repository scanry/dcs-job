package com.six.dcsjob;

import com.six.dcsjob.scheduler.SchedulerManager;
import com.six.dcsjob.scheduler.SchedulerManagerImpl;
import com.six.dcsjob.trigger.CronJobTrigger;
import com.six.dcsjob.trigger.JobTrigger;
import com.six.dcsnodeManager.DcsNodeManager;
import com.six.dcsnodeManager.impl.IgniteDcsNodeManager;

import java.util.List;

import com.six.dcsjob.cache.JobRunningCache;
import com.six.dcsjob.executor.ExecutorManager;
import com.six.dcsjob.executor.ExecutorManagerImpl;
import com.six.dcsjob.model.Job;

/**
 * @author liusong
 * @date 2017年8月23日
 * @email 359852326@qq.com
 */

public class DcsJobManager implements JobTrigger, SchedulerManager {

	private DcsNodeManager dcsNodeManager;
	private JobTrigger jobTrigger;
	private SchedulerManager schedulerManager;
	private ExecutorManager executorManager;
	private JobRunningCache jobRunningCache;

	public DcsJobManager(DcsJobServiceConfig config) {
		dcsNodeManager = new IgniteDcsNodeManager(config.getIp(),
				config.getPort(), config.getNodeStrs());
		schedulerManager = new SchedulerManagerImpl(dcsNodeManager, jobRunningCache, config.getQueryJob(),
				config.getJobRelationshipQuery(), config.getJobSnapshotReport());
		executorManager = new ExecutorManagerImpl(dcsNodeManager, jobRunningCache, config.getWorkSize(),
				config.getWorkerParameterAssembling());
		jobTrigger = new CronJobTrigger(config.getScheduleThreadCount(), schedulerManager);
	}

	@Override
	public void execute(String trigger, String jobName) {
		schedulerManager.execute(trigger, jobName);
	}

	@Override
	public void suspend(String jobName) {
		schedulerManager.suspend(jobName);
	}

	@Override
	public void goOn(String jobName) {
		schedulerManager.goOn(jobName);
	}

	@Override
	public void stop(String jobName) {
		schedulerManager.stop(jobName);
	}

	@Override
	public void stopAll() {
		schedulerManager.stopAll();
	}

	@Override
	public boolean isSchedule(String jobName) {
		return jobTrigger.isSchedule(jobName);
	}

	@Override
	public void schedule(Job job) {
		jobTrigger.schedule(job);
	}

	@Override
	public void schedule(List<Job> jobs) {
		jobTrigger.schedule(jobs);
	}

	@Override
	public void unschedule(String jobName) {
		jobTrigger.unschedule(jobName);
	}

	@Override
	public void askEnd(String jobName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public JobRunningCache getJobRunningCache() {
		return jobRunningCache;
	}

	@Override
	public void shutdown() {
		if (null != executorManager) {
			executorManager.shutdown();
		}
		if (null != schedulerManager) {
			schedulerManager.shutdown();
		}
		if (null != dcsNodeManager) {
			dcsNodeManager.shutdown();
		}

	}
}
