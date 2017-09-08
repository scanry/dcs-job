package com.six.dcsjob.scheduler;

import com.six.dcsjob.model.JobSnapshot;

/**   
* @author liusong  
* @date   2017年8月22日 
* @email  359852326@qq.com 
*/
public interface JobSnapshotReport {

	void report(JobSnapshot jobSnapshot);
}
