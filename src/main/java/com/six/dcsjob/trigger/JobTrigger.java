package com.six.dcsjob.trigger;

import java.util.List;

import com.six.dcsjob.model.Job;

/**
 * @author liusong
 * @date 2017年9月7日
 * @email 359852326@qq.com
 */
public interface JobTrigger {

	boolean isSchedule(String jobName);

	void schedule(Job job);

	void schedule(List<Job> jobs);

	void unschedule(String jobName);

	void shutdown();
}
