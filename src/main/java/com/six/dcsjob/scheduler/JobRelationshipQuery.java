package com.six.dcsjob.scheduler;

import java.util.List;

import com.six.dcsjob.model.JobRelationship;

/**   
* @author liusong  
* @date   2017年8月23日 
* @email  359852326@qq.com 
*/
public interface JobRelationshipQuery {

	List<JobRelationship> get(String jobName);
}
