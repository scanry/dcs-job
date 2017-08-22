package com.six.dcsjob.work;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.six.dcsjob.Job;
import com.six.dcsjob.JobSpace;
import com.six.dcsjob.WorkSpaceData;
import com.six.dcsjob.WorkerErrMsg;
import com.six.dcsjob.WorkerSnapshot;
import com.six.dcsjob.WorkerStatus;
import com.six.dcsjob.executor.ExecutorManager;
import com.six.dcsjob.work.exception.WorkerException;
import com.six.dcsjob.work.exception.WorkerInitException;
import com.six.dcsjob.work.exception.WorkerOtherException;

/**
 * @author sixliu E-mail:359852326@qq.com
 * @version 创建时间：2016年8月29日 下午8:16:06 类说明
 * 
 *          job's worker 抽象基础类，实现了基本了的流程控制
 */
public abstract class AbstractWorker<T extends WorkSpaceData, R extends WorkerSnapshot> implements Worker<T, R> {

	private final static Logger log = LoggerFactory.getLogger(AbstractWorker.class);

	private final static String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
	// 工作默认rest状态下 休息时间
	private final static int DEFAULT_REST_TIME = 2000;

	private long restTime;
	// 检查是否销毁的延迟
	private final static int CHECK_DESTROY_DELAY = 3000;
	// 需要导出异常信息时，异常信息的最大数量
	private final static int REPORT_ERR_MSG_MAX = 20;

	private final static long REPORT_FREQUENCY = 1000 * 30;
	// 当前work线程
	private Thread workThread;
	// 用来lock 读写 状态
	private final StampedLock setStateLock = new StampedLock();
	// 用来lock Condition.await() 和condition.signalAll();
	private final ReentrantLock reentrantLock = new ReentrantLock();
	// 用来Condition.await() 和condition.signalAll();
	private final Condition condition = reentrantLock.newCondition();
	// worker的上级调度管理者
	private ExecutorManager executorManager;
	// worker的上级job
	private Job job;
	// 任务空间
	private JobSpace<T> jobSpace;
	// worker状态
	private volatile WorkerStatus state = WorkerStatus.READY;// 状态
	// worker快照
	private volatile R workerSnapshot;
	// 工作线程上次活动时间记录
	private long lastActivityTime;
	// 异常消息导出
	private WorkerErrMsgExport recordReport;
	private long lastReport;// 上一次Report 时间

	private List<WorkerErrMsg> errMsgs = new ArrayList<>();// job运行记录 异常集合

	@Override
	public void init(Job job) {
		if (compareAndSetState(WorkerStatus.READY, WorkerStatus.INIT)) {
			// String workerName = "job[" + job.getName() +
			// "]_"+jobSnapshotId+"_"+System.currentTimeMillis();
			this.job = job;
			MDC.put("jobName", job.getName());
			try {
				/** 获取执行器manager **/
				this.executorManager = job.getParam(JobParameters.EXECUTOR_MANAGER);
				/** 获取记录导出器 **/
				this.recordReport = job.getParam(JobParameters.RECORD_REPORT);
				/** 获取job空间 **/
				this.jobSpace = job.getParam(JobParameters.JOB_SPACE);
				/** 获取worker运行快照 **/
				this.workerSnapshot = job.getParam(JobParameters.JOB_SPACE);
				restTime = job.getRestTime() == 0 ? DEFAULT_REST_TIME : job.getRestTime();
				initWorker();
				getAndSetState(WorkerStatus.INITED);
			} catch (Exception e) {
				throw new WorkerInitException("job[" + getJob().getName() + "]'s work[" + getName() + "] init err", e);
			}
		}
	}

	/**
	 * 内部初始化方法，用于实现业务相关初始化工作
	 * 
	 * @param jobSnapshot
	 */
	protected abstract void initJob();

	/**
	 * 内部初始化方法，用于实现业务相关初始化工作
	 * 
	 * @param jobSnapshot
	 */
	protected abstract void initWorker();

	/**
	 * 内部工作方法,用于实现相关业务
	 * 
	 * @param workerData
	 * @throws Exception
	 */
	protected abstract void process(T data) throws WorkerException;

	/**
	 * 内部异常处理方法，用来处理业务相关异常
	 * 
	 * @param t
	 * @param workerData
	 */
	protected abstract void onError(Exception t, T data);

	/**
	 * 内部业务销毁方法，在工作结束后调用
	 */
	protected abstract void insideDestroy();

	/**
	 * 工作流程处理中所有不可控异常都会设置为stop退出当前处理
	 */
	private final void work() {
		workThread = Thread.currentThread();
		String nowTime = DateFormatUtils.format(System.currentTimeMillis(), DATE_FORMAT);
		workerSnapshot.setStartTime(nowTime);
		workerSnapshot.setEndTime(nowTime);
		// worker业务处理循环前将workerSnapshot先保存，最后业务流程结束时需要再次更新
		jobSpace.updateWorkerSnapshot(workerSnapshot);
		log.info("start init:" + getName());
		try {
			init(null);
		} catch (WorkerException e) {
			getAndSetState(WorkerStatus.STOP);
			String errMsg = "job[" + getJob().getName() + "]'s work[" + getName() + "] init err";
			log.error(errMsg, e);
			doErr(new WorkerInitException(errMsg, e));
		}
		log.info("end init:" + getName());
		log.info("start work:" + getName());
		compareAndSetState(WorkerStatus.START, WorkerStatus.STARTED);
		while (true) {
			try {
				jobSpace.updateWorkerSnapshot(workerSnapshot);
				// 运行状态时会从队列里获取数据然后进行处理，如果没有获取到数据那么状态改为wait
				if (getState() == WorkerStatus.STARTED) {
					T data = jobSpace.pull();
					if (null != data) {
						doStart(data);
					} else {
						// wait状态只有worker自身获取不到数据时设置
						compareAndSetState(WorkerStatus.STARTED, WorkerStatus.WAIT);
					}
					// 休息状态时会检查工作队列是否为空，如果不为空那么状态改为start,否则休息默认时间
				} else if (getState() == WorkerStatus.REST) {
					if (getJobSpace().doingSize() > 0) {
						compareAndSetState(WorkerStatus.REST, WorkerStatus.STARTED);
					} else {
						signalWait(restTime);
					}
					// wait状态时会询问管理者是否end，然后休息
				} else if (getState() == WorkerStatus.WAIT) {
					if (compareAndSetState(WorkerStatus.WAIT, WorkerStatus.WAITED)) {
						executorManager.askEnd(getJob().getName(), getName());
						signalWait(0);
					}
					// suspend状态时会直接休息
				} else if (getState() == WorkerStatus.SUSPEND) {
					if (compareAndSetState(WorkerStatus.SUSPEND, WorkerStatus.SUSPENDED)) {
						signalWait(0);
					}
					// stop状态时会break
				} else if (getState() == WorkerStatus.STOP) {
					compareAndSetState(WorkerStatus.STOP, WorkerStatus.STOPED);
					workerSnapshot.setNormalEnd(false);
					break;
					// finish状态时会break
				} else if (getState() == WorkerStatus.FINISH) {
					compareAndSetState(WorkerStatus.FINISH, WorkerStatus.FINISHED);
					workerSnapshot.setNormalEnd(true);
					break;
				}
				reportErrMsg(false);
			} catch (Exception e) {
				getAndSetState(WorkerStatus.STOP);
				String errMsg = "job[" + getJob().getName() + "]'s work[" + getName() + "] unkown err and will stop";
				log.error(errMsg, e);
				doErr(new WorkerOtherException(errMsg, e));
			}
		}

		workerSnapshot.setEndTime(DateFormatUtils.format(System.currentTimeMillis(), DATE_FORMAT));
		if (workerSnapshot.getTotalProcessCount() > 0) {
			workerSnapshot
					.setAvgProcessTime(workerSnapshot.getTotalProcessTime() / workerSnapshot.getTotalProcessCount());
		}
		reportErrMsg(true);
		jobSpace.updateWorkerSnapshot(workerSnapshot);
		log.info("end work:" + getName());
	}

	/**
	 * <p>
	 * 更新缓存中WorkSnapshot并且 Report WorkSnapshot
	 * <p>
	 * 前提 null != errMsgs&&errMsgs.size() > 0：
	 * </p>
	 * <p>
	 * 当isSaveErrMsg==true时会将异常消息保存
	 * </p>
	 * <p>
	 * 或者当errMsgs.size() >= SAVE_ERR_MSG_MAX时会将异常消息保存
	 * ,(从异常消息数量去写入，并免内存中大量消息没有被处理)
	 * </p>
	 * <p>
	 * 或者当LastReport >= WORKER_SNAPSHOT_REPORT_FREQUENCY时会将异常消息保存
	 * </p>
	 * 
	 * @param workerSnapshot
	 * @param isSaveErrMsg
	 */
	private void reportErrMsg(boolean isEnd) {
		long nowTime = 0;
		if (errMsgs.size() > 0 && (isEnd || errMsgs.size() >= REPORT_ERR_MSG_MAX
				|| (nowTime = System.currentTimeMillis()) - lastReport >= REPORT_FREQUENCY)) {
			recordReport.export(errMsgs);
			errMsgs.clear();
			lastReport = nowTime;
		}
	}

	private void doStart(T data) {
		long processTime = System.currentTimeMillis();
		try {
			process(data);
			jobSpace.ack(data);
		} catch (WorkerException e) {
			log.error("worker process err", e);
			doErr(e);
			onError(e, data);
		} catch (Exception e) {
			log.error("worker process err", e);
			doErr(new WorkerOtherException(e));
			onError(e, data);
		}
		// 统计次数加1
		workerSnapshot.setTotalProcessCount(workerSnapshot.getTotalProcessCount() + 1);
		processTime = System.currentTimeMillis() - processTime;
		workerSnapshot.setTotalProcessTime((int) (workerSnapshot.getTotalProcessTime() + processTime));
		if (processTime > workerSnapshot.getMaxProcessTime()) {
			workerSnapshot.setMaxProcessTime((int) processTime);
		} else if (processTime < workerSnapshot.getMinProcessTime()) {
			workerSnapshot.setMinProcessTime((int) processTime);
		}
		workerSnapshot.setAvgProcessTime(workerSnapshot.getTotalProcessTime() / workerSnapshot.getTotalProcessCount());
		lastActivityTime = System.currentTimeMillis();
	}

	private void doErr(WorkerException e) {
		String msg = ExceptionUtils.getStackTrace(e);
		workerSnapshot.setErrCount(workerSnapshot.getErrCount() + 1);
		WorkerErrMsg errMsg = new WorkerErrMsg();
		errMsg.setJobSnapshotId(workerSnapshot.getJobSnapshotId());
		errMsg.setJobName(job.getName());
		errMsg.setWorkerName(getName());
		errMsg.setStartTime(DateFormatUtils.format(System.currentTimeMillis(), DATE_FORMAT));
		errMsg.setType(e.getType());
		errMsg.setMsg(msg);
		errMsgs.add(errMsg);
	}

	@Override
	public final void start() {
		if (compareAndSetState(WorkerStatus.READY, WorkerStatus.START)) {
			log.info("worker[" + getName() + "] will start");
			work();
		}
	}

	@Override
	public final void rest() {
		if (compareAndSetState(WorkerStatus.WAITED, WorkerStatus.REST)) {
			signalRun();
			log.info("worker[" + getName() + "] will rest");
		}
	}

	@Override
	public final void suspend() {
		if (compareAndSetState(WorkerStatus.STARTED, WorkerStatus.SUSPEND)) {
			log.info("worker[" + getName() + "] will suspend");
		}
	}

	@Override
	public final void goOn() {
		if (compareAndSetState(WorkerStatus.SUSPENDED, WorkerStatus.STARTED)
				|| compareAndSetState(WorkerStatus.WAITED, WorkerStatus.STARTED)) {
			log.info("worker[" + getName() + "] will goOn");
			signalRun();
		}
	}

	@Override
	public final void stop() {
		WorkerStatus snapshot = getAndSetState(WorkerStatus.STOP);
		log.info("worker will stop:" + getName());
		// 如果当前状态属于暂停或者等待下那么signalRun
		if (snapshot == WorkerStatus.SUSPENDED || snapshot == WorkerStatus.WAITED) {
			signalRun();
		}
		if (this.state != WorkerStatus.STOPED) {
			try {
				Thread.sleep(CHECK_DESTROY_DELAY);
			} catch (InterruptedException e) {
			}
			// 如果3秒延迟后状态还没有变成DESTROY，那么有可能阻塞了，调用workThread.interrupt();
			if (this.state != WorkerStatus.STOPED) {
				// 调用当前线程interrupt,并免因为io网络等其他原因导致阻塞无法stop
				workThread.interrupt();
			}
		}
	}

	@Override
	public void finish() {
		WorkerStatus snapshot = getAndSetState(WorkerStatus.FINISH);
		log.info("worker will finish:" + getName());
		if (snapshot == WorkerStatus.SUSPEND || snapshot == WorkerStatus.WAITED) {
			signalRun();
		}
	}

	/**
	 * 控制工作线程wait,restTime=0时为一直等待直到被唤醒,大于0为等待指定时间后恢复运行
	 * 
	 * @param restTime
	 */
	private void signalWait(long restTime) {
		if ((getState() == WorkerStatus.SUSPEND || getState() == WorkerStatus.REST
				|| getState() == WorkerStatus.WAITED)) {
			reentrantLock.lock();
			try {
				if ((getState() == WorkerStatus.SUSPEND || getState() == WorkerStatus.REST
						|| getState() == WorkerStatus.WAITED)) {
					if (restTime > 0) {
						condition.await(restTime, TimeUnit.MILLISECONDS);
					} else {
						condition.await();
					}
				}
			} catch (InterruptedException e) {
				log.error("worker wait err", e);
			} finally {
				reentrantLock.unlock();
			}
		}
	}

	/**
	 * 通知工作线程恢复运行
	 */
	private void signalRun() {
		if (getState() == WorkerStatus.STARTED || getState() == WorkerStatus.REST || getState() == WorkerStatus.STOP
				|| getState() == WorkerStatus.STOPED || getState() == WorkerStatus.FINISH
				|| getState() == WorkerStatus.FINISHED) {
			reentrantLock.lock();
			try {
				if (getState() == WorkerStatus.STARTED || getState() == WorkerStatus.REST
						|| getState() == WorkerStatus.STOP || getState() == WorkerStatus.STOPED
						|| getState() == WorkerStatus.FINISH || getState() == WorkerStatus.FINISHED) {
					condition.signalAll();
				}
			} finally {
				reentrantLock.unlock();
			}
		}
	}

	@Override
	public JobSpace<T> getJobSpace() {
		return jobSpace;
	}

	@Override
	public WorkerStatus getState() {
		long stamp = setStateLock.readLock();
		try {
			return state;
		} finally {
			setStateLock.unlock(stamp);
		}
	}

	/**
	 * 设置 worker状态 ，新至 RunningJobRegisterCenter 并返回之前快照值
	 * 
	 * @param state
	 */
	protected WorkerStatus getAndSetState(WorkerStatus updateState) {
		long stamp = setStateLock.writeLock();
		WorkerStatus snapshot = this.state;
		try {
			this.state = updateState;
			workerSnapshot.setStatus(updateState);
			jobSpace.updateWorkerSnapshot(workerSnapshot);
		} finally {
			setStateLock.unlock(stamp);
		}
		return snapshot;
	}

	/**
	 * 比较是否等于预期状态，然后继续update
	 * 
	 * @param expectState
	 * @param updateState
	 * @return
	 */
	protected boolean compareAndSetState(WorkerStatus expectState, WorkerStatus updateState) {
		boolean result = false;
		if (this.state == expectState) {
			long stamp = setStateLock.writeLock();
			try {
				if (this.state == expectState) {
					this.state = updateState;
					workerSnapshot.setStatus(updateState);
					jobSpace.updateWorkerSnapshot(workerSnapshot);
					result = true;
				}
			} finally {
				setStateLock.unlock(stamp);
			}
		}
		return result;
	}

	@Override
	public R getWorkerSnapshot() {
		return workerSnapshot;
	}

	@Override
	public boolean isRunning() {
		return getState() == WorkerStatus.STARTED;
	}

	@Override
	public Job getJob() {
		return job;
	}

	@Override
	public long getLastActivityTime() {
		return lastActivityTime;
	}

	@Override
	public int hashCode() {
		String name = getName();
		int hash = name.hashCode();
		return hash;
	}

	@Override
	public boolean equals(Object anObject) {
		if (null != anObject && anObject instanceof Worker) {
			Worker<?, ?> worker = (Worker<?, ?>) anObject;
			if (getName().equals(worker.getName())) {
				return true;
			}
		}
		return false;
	}

	@Override
	public final void destroy() {
		getAndSetState(WorkerStatus.DESTROY);
		if (null != jobSpace) {
			try {
				jobSpace.close();
			} catch (Exception e) {
				log.error("workSpace[" + jobSpace.getName() + "] close", e);
			}
		}
		try {
			insideDestroy();
		} catch (Exception e) {
			log.error("worker[" + getName() + "] insideDestroy", e);
		}
		MDC.remove("jobName");
		log.info("destroy worker:" + getName());
		getAndSetState(WorkerStatus.DESTROYED);
	}
}
