package com.six.dcsjob.scheduler;

import com.six.dcsjob.model.Job;

/**   
* @author liusong  
* @date   2017年8月22日 
* @email  359852326@qq.com 
*/
public interface JobQuery {

	Job get(String jobName);
}
