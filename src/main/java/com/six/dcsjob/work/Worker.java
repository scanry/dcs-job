package com.six.dcsjob.work;

import com.six.dcsjob.model.Job;
import com.six.dcsjob.model.WorkerSnapshot;
import com.six.dcsjob.model.WorkerStatus;
import com.six.dcsjob.space.JobSpace;
import com.six.dcsjob.space.WorkSpaceData;

/**
 * @author six
 * @date 2016年1月15日 下午6:20:00
 * 
 *       运行job的worker接口定义
 * 
 */
public interface Worker<T extends WorkSpaceData, R extends WorkerSnapshot>{

	public static final String READY = "ready";// 初始状态为准备
	
	public static final String INIT = "init";// 初始化
	
	public static final String INITED = "inited";// 完成初始化

	public static final String START = "start";// 开始

	public static final String STARTED = "started";// 已经开始

	public static final String WAIT = "wait";// 等待

	public static final String WAITED = "waited";// 已经等待

	public static final String REST = "rest";// 休息

	public static final String SUSPEND = "suspend";// 暂停

	public static final String SUSPENDED = "suspended";// 已经暂停

	public static final String STOP = "stop";// 停止

	public static final String STOPED = "stoped";// 已经停止

	public static final String FINISH = "finish";// 完成

	public static final String FINISHED = "finished";// 已经完成

	public static final String DESTROY = "destroy";// 销毁

	public static final String DESTROYED = "destroyed";// 已经销毁

	/**
	 *初始化
	 */
	void init(Job job);
	/**
	 * 获取 worker name
	 * 
	 * @return
	 */
	String getName();

	/**
	 * 获取work Job
	 * 
	 * @return
	 */
	Job getJob();

	JobSpace<T> getJobSpace();
	/**
	 * 获取最后一次活动时间
	 * 
	 * @return
	 */
	long getLastActivityTime();

	/**
	 * 获取工作运行快照
	 * 
	 * @return
	 */
	R getWorkerSnapshot();

	/**
	 * 开始方法 只有当 state== ready 时调用
	 */
	public void start(Job job);

	/**
	 * 在运行状态下，没有处理数据事等待
	 */
	public void rest();

	/**
	 * 暂停方法 只有在state==stared时候调用
	 */
	public void suspend();

	/**
	 * 继续运行方法只有在state==suspend时调用
	 */
	void goOn();

	/**
	 * 停止方法在任何状态时候都可以调用
	 */
	void stop();

	/**
	 * 完成方法由管理者调用
	 */
	void finish();

	/**
	 * 销毁 方法 最后结束时候调用
	 */
	void destroy();

	/**
	 * 获取状态
	 * 
	 * @return
	 */
	WorkerStatus getState();

	/**
	 * 是否运行
	 * 
	 * @return
	 */
	boolean isRunning();
}
