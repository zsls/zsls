package org.pbccrc.zsls.registry;

/**
 *	监听Registry的可用状态。通常当Registry不可用时不再对外提供服务（单点模式除外），以防止脑裂。
 */
public interface RegistryStateListener {
	
	void serviceEnabled();
	void serviceDisabled();
}
