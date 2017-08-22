package com.six.dcsjob.work;

import java.util.List;

import com.six.dcsjob.WorkerErrMsg;

/**   
* @author liusong  
* @date   2017年8月11日 
* @email  359852326@qq.com 
*/
public interface WorkerErrMsgExport {
	void export(List<WorkerErrMsg> list);
}
