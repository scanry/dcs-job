package com.six.dcsjob.model;

import java.io.Serializable;

import lombok.Data;


/**
 * @author 作者
 * @E-mail: 359852326@qq.com
 * @date 创建时间：2017年2月16日 上午9:32:02
 */
@Data
public class JobSnapshot implements Serializable {

	private static final long serialVersionUID = -5076089473208316846L;
	
	private String id;// 任务名
	private String name;// 任务名
	private volatile int status=JobSnapshotStatus.READY;// 任务状态
	private String triggerJobName;// 触发任务的类型
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
}
