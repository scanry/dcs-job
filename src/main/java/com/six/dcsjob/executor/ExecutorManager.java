package com.six.dcsjob.executor;

import com.six.dcsjob.model.Job;

/**   
* @author liusong  
* @date   2017年8月11日 
* @email  359852326@qq.com 
*/
public interface ExecutorManager {

	/**
	 * 	执行指定job
	 * @param job
	 */
	void execute(Job job,String jobSnapshotId);

	void rest(String jobName);
	/**
	 * 暂停指定job
	 * @param jobName
	 */
	void suspend(String jobName);

	/**
	 * 继续运行指定job
	 * @param jobName
	 */
	void goOn(String jobName);

	/**
	 * 停止指定job
	 * @param jobName
	 */
	void stop(String jobName);
	
	void finish(String jobName);
	
	/**
	 * 停掉所有运行job
	 */
	void stopAll();
	
	void askEnd(String jobName,String workName);
	
	boolean isShutdown();
	/**
	 * shutdown调度器
	 */
	void shutdown();

}
