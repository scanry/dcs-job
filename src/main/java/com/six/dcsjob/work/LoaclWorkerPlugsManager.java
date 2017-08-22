package com.six.dcsjob.work;

/**
 * @author 作者
 * @E-mail: 359852326@qq.com
 * @date 创建时间：2017年5月8日 上午9:45:43
 */
public class LoaclWorkerPlugsManager extends AbstractWorkerPlugsManager {

	@Override
	protected boolean savePlugClassToCache(Class<?> clz) {
		return false;
	}

	@Override
	protected Class<?> findFromCache(String workerClassName) {
		return null;
	}
}
