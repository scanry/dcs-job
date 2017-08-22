package com.six.dcsjob;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;

/**
 * @author sixliu E-mail:359852326@qq.com
 * @version 创建时间：2016年5月19日 下午8:35:28 类说明 爬虫job
 */
@Data
public class Job  implements Serializable {

	private static final long serialVersionUID = 781122651512874550L;

	private String name;// job 名字

	private int level;// 任务级别

	private String designatedNodeName;// 指定节点运行

	private int needNodes;// 工作需要的节点数

	private int threads;// 节点执行任务的线程数

	private int isScheduled;// 是否开启定时

	private String cronTrigger;// cronTrigger 定时

	private long workFrequency ;// 每次处理时间的阈值 默认1000毫秒

	private String workerClass;// worker class

	private String workSpaceName;// 工作空间名称
	
	private long restTime;

	private String describe;// 任务描述

	private String user;// 任务 所属用户

	private Map<String,Object> configure=Collections.emptyMap();;// 任务参数


	@SuppressWarnings("unchecked")
	public <T>T getParam(String paramKey) {
		if (StringUtils.isBlank(paramKey)) {
			throw new NullPointerException("paramKey mustn't be blank");
		}
		Object value=configure.get(paramKey);
		return null!=value?(T)value:null;
	}

	public Object getParamStr(String paramKey, Object defaultParam) {
		Object value=getParam(paramKey);
		return null==value?defaultParam:value;
	}

	public String getParamStr(String paramKey) {
		Object value=getParam(paramKey);
		return null!=value?value.toString():null;
	}

	public String getParam(String paramKey, String defaultParam) {
		String value=getParamStr(paramKey);
		return null==value?defaultParam:value;
	}

	public int getParamInt(String paramKey) {
		String value = getParamStr(paramKey);
		if (null==value) {
			throw new RuntimeException("get paramInt[" + paramKey + "] is blank");
		} else {
			try {
				return Integer.valueOf(value);
			} catch (Exception e) {
				throw new RuntimeException("invalid paramInt[" + paramKey + "]", e);
			}
		}
	}

	public int getParamInt(String paramKey, int defaultParam) {
		try {
			return getParamInt(paramKey);
		} catch (Exception e) {

		}
		return defaultParam;
	}

	public boolean getParamBoolean(String paramKey) {
		return getParamInt(paramKey) == 1 ? true : false;
	}
	
	public boolean getParamBoolean(String paramKey, boolean defaultParam) {
		try {
			return getParamInt(paramKey) == 1 ? true : false;
		} catch (Exception e) {

		}
		return defaultParam;
	}

	
	
	@Override
	public int hashCode() {
		if (null != name) {
			return name.hashCode();
		}
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		if (null != o) {
			if (o instanceof Job) {
				Job targetJob = (Job) o;
				if (null != getName() && getName().equals(targetJob.getName())) {
					return true;
				}
			}
		}
		return false;
	}

}
