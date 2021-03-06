package com.six.dcsjob.scheduler;

import com.six.dcsjob.cache.JobRunningCache;

/**
 * @author liusong
 * @date 2017年8月11日
 * @email 359852326@qq.com
 */
public interface SchedulerManager {
	/**
	 * 执行指定job
	 * 
	 * @param job
	 */
	void execute(String triggerJobName,String jobName);

	/**
	 * 暂停指定job
	 * 
	 * @param jobName
	 */
	void suspend(String jobName);

	/**
	 * 继续运行指定job
	 * 
	 * @param jobName
	 */
	void goOn(String jobName);

	/**
	 * 停止指定job
	 * 
	 * @param jobName
	 */
	void stop(String jobName);

	/**
	 * 停掉所有运行job
	 */
	void stopAll();

	
	void askEnd(String jobName);
	
	JobRunningCache getJobRunningCache();
	/**
	 * shutdown调度器
	 */
	void shutdown();

}
