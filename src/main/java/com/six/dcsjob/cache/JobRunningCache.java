package com.six.dcsjob.cache;

import java.util.List;
import java.util.Map;

import org.apache.ignite.cluster.ClusterNode;

import com.six.dcsjob.model.JobSnapshot;
import com.six.dcsjob.model.WorkerSnapshot;
import com.six.dcsjob.work.Worker;

/**
 * @author liusong
 * @date 2017年8月22日
 * @email 359852326@qq.com job运行缓存
 */
public interface JobRunningCache {

	@FunctionalInterface
	public interface ListenStartProcess{
		void process(WorkerSnapshot workerSnapshot);
	}
	
	@FunctionalInterface
	public interface ListenEndProcess{
		void process(WorkerSnapshot workerSnapshot);
	}
	
	/**
	 * 判断job是否在运行
	 * @return
	 */
	boolean isRunning(String jobName);
	
	/**
	 * 获取运行job的节点集合
	 * @param jobName
	 * @return
	 */
	List<ClusterNode> runningJobNodes(String jobName);
	
	/**
	 * 获取job的运行快照
	 * @param jobName
	 * @param jobSnapshotId
	 * @return
	 */
	JobSnapshot getJobSnapshot(String jobName, String jobSnapshotId);
	
	/**
	 * 注册job运行快照，并监听结束事件
	 * @param jobSnapshot
	 * @param listenProcess
	 */
	void registerAndUpdate(JobSnapshot jobSnapshot);
	
	/**
	 * 移除注册
	 * @param jobName
	 * @param jobSnapshotId
	 */
	void unregister(JobSnapshot jobSnapshot);
	
	/**
	 * 监听执行任务的worker启动事件
	 * @param jobSnapshot
	 * @param listenProcess
	 */
	void listenWorkerStart(JobSnapshot jobSnapshot,ListenStartProcess listenProcess);
	
	/**
	 * 监听执行任务的worker启动事件
	 * @param jobSnapshot
	 * @param listenProcess
	 */
	void listenWorkerEnd(JobSnapshot jobSnapshot,ListenEndProcess listenProcess);
	
	
	/**
	 * 获取本地所有运行的worker
	 * @return
	 */
	Map<String, Map<String, Worker<?, ?>>> getLocalAllWorkers();

	/**
	 * 获取本地指定job所有运行的worker
	 * @return
	 */
	Map<String, Worker<?, ?>> getLocalWorkers(String jobName);

	/**
	 * 注册指定任务运行额worker
	 * @return
	 */
	void registerAndUpdate(String jobName, String jobSnapshotId, Worker<?, ?> worker);
	
	/**
	 * 取消注册指定任务运行额worker
	 * @return
	 */
	void unregister(String jobName, String jobSnapshotId, Worker<?, ?> worker);
	
	/**
	 * 判断本地worker是否全部结束
	 * @return
	 */
	boolean localWorkersIsEnd();
	
}
