package com.six.dcsjob;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * @author 作者
 * @E-mail: 359852326@qq.com
 * @date 创建时间：2017年2月16日 上午9:32:02
 */
public class JobSnapshot implements Serializable {

	private static final long serialVersionUID = -5076089473208316846L;
	
	private String id;// 任务名
	private String name;// 任务名
	private volatile int status=JobSnapshotStatus.READY;// 任务状态
	private TriggerType triggerType;// 触发任务的类型
	private String workSpaceName;// 任务工作空间名
	private String startTime;// 开始时间
	private String endTime;//结束时间
	private int isScheduled;//
	private int workSpaceDoingSize;// 任务队列数量
	private int workSpaceErrSize;// 错误任务队列数量
	private int totalProcessCount;// 统计处理多少个数据
	private int totalResultCount;// 统计获取多少个数据
	private int totalProcessTime;// 统计获取处理时间
	private int avgProcessTime;// 平均每次任务处理时间
	private int maxProcessTime;// 最大任务处理时间
	private int minProcessTime;// 最小任务处理时间
	private int errCount;// 异常次数

	private Map<String, String> runtimeParamMap = new HashMap<String, String>();

	public JobSnapshot() {
	}

	public JobSnapshot(String id,String name) {
		this.id=id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}


	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public TriggerType getTriggerType() {
		return triggerType;
	}

	public void setTriggerType(TriggerType triggerType) {
		this.triggerType = triggerType;
	}

	public String getWorkSpaceName() {
		return workSpaceName;
	}

	public void setWorkSpaceName(String workSpaceName) {
		this.workSpaceName = workSpaceName;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public int getIsScheduled() {
		return isScheduled;
	}

	public void setIsScheduled(int isScheduled) {
		this.isScheduled = isScheduled;
	}

	public int getWorkSpaceDoingSize() {
		return workSpaceDoingSize;
	}

	public void setWorkSpaceDoingSize(int workSpaceDoingSize) {
		this.workSpaceDoingSize = workSpaceDoingSize;
	}

	public int getWorkSpaceErrSize() {
		return workSpaceErrSize;
	}

	public void setWorkSpaceErrSize(int workSpaceErrSize) {
		this.workSpaceErrSize = workSpaceErrSize;
	}

	public int getTotalProcessCount() {
		return totalProcessCount;
	}

	public void setTotalProcessCount(int totalProcessCount) {
		this.totalProcessCount = totalProcessCount;
	}

	public int getTotalResultCount() {
		return totalResultCount;
	}

	public void setTotalResultCount(int totalResultCount) {
		this.totalResultCount = totalResultCount;
	}

	public int getTotalProcessTime() {
		return totalProcessTime;
	}

	public void setTotalProcessTime(int totalProcessTime) {
		this.totalProcessTime = totalProcessTime;
	}

	public int getAvgProcessTime() {
		return avgProcessTime;
	}

	public void setAvgProcessTime(int avgProcessTime) {
		this.avgProcessTime = avgProcessTime;
	}

	public int getMaxProcessTime() {
		return maxProcessTime;
	}

	public void setMaxProcessTime(int maxProcessTime) {
		this.maxProcessTime = maxProcessTime;
	}

	public int getMinProcessTime() {
		return minProcessTime;
	}

	public void setMinProcessTime(int minProcessTime) {
		this.minProcessTime = minProcessTime;
	}

	public int getErrCount() {
		return errCount;
	}

	public void setErrCount(int errCount) {
		this.errCount = errCount;
	}

	public String getParam(String key) {
		return this.runtimeParamMap.get(key);
	}

	public void putParam(String key, String param) {
		runtimeParamMap.put(key, param);
	}

	public String toString() {
		StringBuilder sbd = new StringBuilder();
		sbd.append("id:").append(id).append(",");
		sbd.append("name:").append(name).append(",");
		sbd.append("startTime:").append(startTime).append(",");
		sbd.append("endTime:").append(endTime).append(",");
		sbd.append("totalProcessCount:").append(totalProcessCount).append(",");
		sbd.append("totalResultCount:").append(totalResultCount).append(",");
		sbd.append("avgProcessTime:").append(avgProcessTime).append(",");
		sbd.append("maxProcessTime:").append(maxProcessTime).append(",");
		sbd.append("minProcessTime:").append(minProcessTime).append(",");
		sbd.append("errCount:").append(errCount).append(",");
		return sbd.toString();
	}
}
