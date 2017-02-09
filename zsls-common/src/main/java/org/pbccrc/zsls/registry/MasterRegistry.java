package org.pbccrc.zsls.registry;

/**
 *  Registry interface more specific for master election. 
 *  a quite simple rule: 
 *  	each jobtracker would try be to master on starting up, but should failed if 
 *  	there is already one exist, in which case the jobtracker becomes standby.
 */
public interface MasterRegistry {
	
	/* return true，说明成为master， false说明已经存在master而成为备，其他运行时异常直接抛出 */
	boolean registerMaster(RegNode node);							// 尝试注册为master
	
	RegNode getMaster();											// 读取当前master
	
	void substribe(NotifyListener listener);						// 订阅master信息
	
	void addServiceListener(RegistryStateListener listener);		// 添加注册中心可用性的监听
	
}
