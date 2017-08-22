package com.six.dcsjob.work;


/**   
* @author liusong  
* @date   2017年8月22日 
* @email  359852326@qq.com 
*/
public interface WorkerPlugsManager {

	/**
	 * 保存worker 插件 class
	 * 
	 * @param workerClassName
	 * @param classByte
	 * @return
	 */
	public boolean saveClass(Class<?> clz);

	/**
	 * 获取worker 插件 class
	 * 
	 * @param workerClassName
	 * @return
	 */
	public Worker<?, ?>[] newWorker(String workerClass,int num);
}
