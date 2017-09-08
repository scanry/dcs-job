package com.six.dcsjob;

import java.util.List;

import com.six.dcsjob.executor.WorkerParameterAssembling;
import com.six.dcsjob.scheduler.JobSnapshotReport;
import com.six.dcsjob.scheduler.JobQuery;
import com.six.dcsjob.scheduler.JobRelationshipQuery;

import lombok.Data;

/**   
* @author liusong  
* @date   2017年8月23日 
* @email  359852326@qq.com 
*/
@Data
public class DcsJobServiceConfig {

	List<String> nodeStrs;
	/** 节点地址 **/
	private String ip;
	/** 节点端口 **/
	private int port;
	long keepliveInterval;
	String zkConnection;
	int nodeRpcServerThreads;
	int nodeRpcClientThreads;
	JobQuery queryJob;
	JobSnapshotReport jobSnapshotReport;
	int scheduleThreadCount;
	int workSize;
	WorkerParameterAssembling workerParameterAssembling;
	JobRelationshipQuery jobRelationshipQuery;
}
