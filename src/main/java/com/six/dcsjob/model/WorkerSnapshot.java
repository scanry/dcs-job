package com.six.dcsjob.model;

import java.io.Serializable;

import lombok.Data;


/**
 * @author 作者
 * @E-mail: 359852326@qq.com
 * @date 创建时间：2016年10月19日 下午12:10:57 job job运行信息
 */
@Data
public class WorkerSnapshot implements Serializable {

	private static final long serialVersionUID = 8013640891595004526L;
	private String jobSnapshotId;// jobSnapshotId id
	private String name;// worker 名
	private String localNode;// 本地节点
	private String jobName;// 任务名
	private boolean normalEnd;//是否正常结束
	private volatile WorkerStatus status = WorkerStatus.READY;// 状态
	private String startTime = "";// 开始时间
	private String endTime = "";// 结束时间
	private int totalProcessCount;// 统计处理多少个数据
	private int totalResultCount;// 统计获取多少个数据
	private int totalProcessTime;// 统计处理时间
	private int maxProcessTime;// 最大任务处理时间
	private int minProcessTime = 999999999;// 最小任务处理时间 默认为很大的数字
	private int avgProcessTime;// 平均每次任务处理时间
	private int errCount;// 任务异常数量
}
