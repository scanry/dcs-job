package com.six.dcsjob.scheduler;

import com.six.dcsjob.Job;

/**
 * @author liusong
 * @date 2017年8月11日
 * @email 359852326@qq.com
 */
public interface Scheduler {
	/**
	 * 执行指定job
	 * 
	 * @param job
	 */
	void execute(Job job);

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

	/**
	 * 指定job下的worker结束
	 * 
	 * @param jobName
	 * @param workerName
	 */
	void end(String jobName, String workerName);

	/**
	 * 定时调度指定job
	 * 
	 * @param job
	 */
	void schedule(Job job);

	/**
	 * 取消定时调度指定job
	 * 
	 * @param jobName
	 */
	void unSchedule(String jobName);

	/**
	 * shutdown调度器
	 */
	void shutdown();

}
