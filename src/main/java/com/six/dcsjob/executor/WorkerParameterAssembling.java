package com.six.dcsjob.executor;

import com.six.dcsjob.Job;

/**
 * @author liusong
 * @date 2017年8月22日
 * @email 359852326@qq.com worker参数装配
 */
public interface WorkerParameterAssembling {

	void setParameter(Job job);
}
