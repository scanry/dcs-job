package com.six.dcsjob.space;

import com.six.dcsjob.model.Index;

/**
 * @author 作者
 * @E-mail: 359852326@qq.com
 * @date 创建时间：2017年3月29日 下午1:09:37
 * 
 *       使用WorkSpace时 处理数据 必须实现此接口
 */
public interface WorkSpaceData {

	public void setIndex(Index index);
	
	public Index getIndex();
	
	public String getKey();
}
