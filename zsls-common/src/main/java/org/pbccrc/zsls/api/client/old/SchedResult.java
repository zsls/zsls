package org.pbccrc.zsls.api.client.old;

public class SchedResult {
	
	public static enum RetCode {
		/*服务器置位*/
		OK,				// 接收成功，并伴随着generatedId.
		ERROR,			// 服务器接收失败，应该重试
		INVALID,		// 客户端数据无效
	}
	
	public SchedResult(InnerSchedResult ret) {
		retCode = ret.retCode;
		info = ret.info;
		generatedId = ret.generatedId;
	}
	
	public String generatedId;
	
	public RetCode retCode;
	
	public String info;
	
	public String toString() {
		return "retCode: " + retCode.name() + " , id: " + generatedId + ", info: " + info;
	}

}
