package com.six.dcsjob.cache;

import java.util.Map;

import com.six.dcsjob.work.Worker;

/**   
* @author liusong  
* @date   2017年8月22日 
* @email  359852326@qq.com 
*/
public interface WorkerRunningCache {

	/**
	 * 获取所有运行的worker
	 * @return
	 */
	Map<String, Map<String, Worker<?, ?>>> getAllWorkers();

	Map<String, Worker<?, ?>> getWorkers(String jobName);

	boolean isEmpty();

	void register(String jobName, String jobSnapshotId, Worker<?, ?> worker);
	
	void unregister(String jobName, String jobSnapshotId, Worker<?, ?> worker);
}
