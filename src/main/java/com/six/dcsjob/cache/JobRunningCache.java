package com.six.dcsjob.cache;

import java.util.List;

import com.six.dcsjob.JobSnapshot;
import com.six.dcsnodeManager.Node;

/**
 * @author liusong
 * @date 2017年8月22日
 * @email 359852326@qq.com job运行缓存
 */
public interface JobRunningCache {

	@FunctionalInterface
	public interface ListenProcess{
		void process();
	}
	
	/**
	 * 判断job是否在运行
	 * @return
	 */
	boolean isRunning(String jobName);
	
	List<Node> runningJobNodes(String jobName);
	
	void register(JobSnapshot jobSnapshot);
	
	void listenJobIsEnd(String jobName, String jobSnapshotId,ListenProcess listenProcess);

	
}
