package com.six.dcsjob.model;

/**
 * @author 作者
 * @E-mail: 359852326@qq.com
 * @date 创建时间：2016年10月12日 上午10:29:28
 */
public interface JobSnapshotStatus {

	/*** 准备 */
	int READY = 0;
	/*** 定时计划中 */
	int SCHEDULE = 1;
	/*** 等待被执行 */
	int PENDING_EXECUTED = 2;
	/** * 初始化 */
	int INIT = 3;
	/** * 运行 */
	int EXECUTING = 4;
	/*** 暂停 */
	int SUSPEND = 5;
	/*** 结束 */
	int END=6;
	/*** 停止 */
	int STOP = 7;
	/*** 完成 */
	int FINISHED = 8;
}
