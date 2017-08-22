package com.six.dcsjob;

import java.io.Serializable;

import com.six.dcsjob.work.Worker;

/**
 * job's worker状态 枚举
 * 
 * @author six
 * @email 359852326@qq.com
 */
public enum WorkerStatus implements Serializable {
	
	/**准备**/
	READY(Worker.READY),
	
	/**初始化**/
	INIT(Worker.INIT),
	
	/**完成初始化**/
	INITED(Worker.INITED),
	
	/**开始**/
	START(Worker.START),
	/**已经开始**/
	STARTED(Worker.STARTED),

	/**等待**/
	WAIT(Worker.WAIT),
	/**已经等待**/
	WAITED(Worker.WAITED),

	/**休息**/
	REST(Worker.REST),

	/**暂停**/
	SUSPEND(Worker.SUSPEND),
	/**已经暂停**/
	SUSPENDED(Worker.SUSPENDED),

	/**停止**/
	STOP(Worker.STOP),
	/**已经停止**/
	STOPED(Worker.STOPED),

	/**完成**/
	FINISH(Worker.FINISH),
	/**已经完成**/
	FINISHED(Worker.FINISHED),

	/**销毁**/
	DESTROY(Worker.DESTROY),
	/**已经销毁**/
	DESTROYED(Worker.DESTROYED);
	

	private final String event;

	private WorkerStatus(String event) {
		this.event = event;
	}

	public String getEvent() {
		return event;
	}
}
