package com.six.dcsjob;

/**
 * @author sixliu E-mail:359852326@qq.com
 * @version 创建时间：2016年1月16日 上午7:12:58 当任务执行时，都会拥有自己的工作空间,工作空间处理的数据必须实现
 *          WorkSpaceData 接口
 */
public interface JobSpace<T extends WorkSpaceData> {

	/**
	 * 工作空间名称
	 * 
	 * @return
	 */
	String getName();
	
	JobSnapshot getJobSnapshot();
	
	void updateJobSnapshot(JobSnapshot jobSnapshot);
	
	void updateWorkerSnapshot(WorkerSnapshot workerSnapshot);
	/**
	 * 将数据 推到工作队列中
	 * 
	 * @param data
	 * @return 成功或失败
	 */
	boolean push(T data);

	/**
	 * 从工作队列拉取数据
	 * <p>
	 * 注意:拉去数据并没有删除实际数据，需要结合ack一起使用
	 * </p>
	 * 
	 * @return 有数据返回队列头数据，否则返回null
	 */
	T pull();

	/**
	 * 确认pull出来得数据被成功处理，然后删除实际数据
	 * 
	 * @param data
	 */
	void ack(T data);

	
	void addErr(T data);
	/**
	 * 获取工作队列size
	 * 
	 * @return
	 */
	int doingSize();
	
	/**
	 * 获取工作异常队列size
	 * 
	 * @return
	 */
	int errSize();
	
	/**
	 * 清除工作队列
	 */
	void clearDoing();

	/**
	 * 清除工作异常队列
	 */
	void clearErr();
	/**
	 * 关闭
	 */
	void close();
}
