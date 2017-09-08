package com.six.dcsjob.trigger;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
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

import com.six.dcsjob.model.Job;
import com.six.dcsjob.scheduler.SchedulerManager;

/**
 * @author liusong
 * @date 2017年9月7日
 * @email 359852326@qq.com
 */
public class CronJobTrigger implements JobTrigger {

	private static final String JOB_NAME_KEY = "jobName";
	private static final String SCHEDULER_MANAGER_KEY = "scheduleManager";
	private final static String schedulerGroup = "crawler";
	private final static String triggerType = "CronJobTrigger";
	private int threadCount;
	private Scheduler scheduler;
	private SchedulerManager schedulerManager;

	public CronJobTrigger(int threadCount, SchedulerManager schedulerManager) {
		this.schedulerManager = schedulerManager;
		this.threadCount = threadCount;
		initQuartzScheduler();
	}

	private void initQuartzScheduler() {
		Properties props = new Properties();
		props.put("org.quartz.scheduler.instanceName", "DefaultQuartzScheduler");
		props.put("org.quartz.scheduler.rmi.export", false);
		props.put("org.quartz.scheduler.rmi.proxy", false);
		props.put("org.quartz.scheduler.wrapJobExecutionInUserTransaction", false);
		props.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
		props.put("org.quartz.threadPool.threadCount", String.valueOf(threadCount));
		props.put("org.quartz.threadPool.threadPriority", "5");
		props.put("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread", true);
		props.put("org.quartz.jobStore.misfireThreshold", "60000");
		try {
			StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory(props);
			scheduler = stdSchedulerFactory.getScheduler();
			scheduler.start();
		} catch (SchedulerException e) {
			throw new RuntimeException("init queartz scheduler err", e);
		}
	}

	public static class ScheduledJob implements org.quartz.Job {
		@Override
		public void execute(JobExecutionContext context) throws JobExecutionException {
			SchedulerManager schedulerManager = (SchedulerManager) context.getJobDetail().getJobDataMap()
					.get(JOB_NAME_KEY);
			String jobName = (String) context.getJobDetail().getJobDataMap().get(JOB_NAME_KEY);
			schedulerManager.execute(triggerType, jobName);
		}
	}

	@Override
	public boolean isSchedule(String jobName) {
		if (StringUtils.isNotBlank(jobName)) {
			synchronized (scheduler) {
				JobKey jobKey = new JobKey(jobName, schedulerGroup);
				try {
					return scheduler.checkExists(jobKey);
				} catch (SchedulerException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return false;
	}

	/**
	 * 向调度器注册job
	 * 
	 * @param job
	 */
	@Override
	public void schedule(Job job) {
		if (null != job
				&&StringUtils.isNotBlank(job.getCronTrigger())&&
				!isSchedule(job.getName())) {
			synchronized (scheduler) {
				JobKey jobKey = new JobKey(job.getName(), schedulerGroup);
				Trigger trigger = TriggerBuilder.newTrigger().withIdentity(job.getName(), schedulerGroup)
						.withSchedule(CronScheduleBuilder.cronSchedule(job.getCronTrigger())).startNow()
						.build();
				JobBuilder jobBuilder = JobBuilder.newJob(ScheduledJob.class);
				jobBuilder.withIdentity(jobKey);
				JobDataMap newJobDataMap = new JobDataMap();
				newJobDataMap.put(JOB_NAME_KEY, job.getName());
				newJobDataMap.put(SCHEDULER_MANAGER_KEY, schedulerManager);
				jobBuilder.setJobData(newJobDataMap);
				JobDetail jobDetail = jobBuilder.build();
				try {
					scheduler.scheduleJob(jobDetail, trigger);
				} catch (SchedulerException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public void schedule(List<Job> jobs) {
		if (null != jobs) {
			for (Job job : jobs) {
				schedule(job);
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
					throw new RuntimeException(e);
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
				throw new RuntimeException(e);
			}
		}
	}
}
