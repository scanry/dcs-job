package com.six.dcsjob.model;
/**   
* @author liusong  
* @date   2017年8月23日 
* @email  359852326@qq.com 
*/

import lombok.Data;

@Data
public class JobRunningContext {

	private JobSnapshot jobSnapshot;
	private Job job;
	
}
