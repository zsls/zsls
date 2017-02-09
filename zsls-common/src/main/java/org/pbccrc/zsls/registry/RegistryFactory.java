package org.pbccrc.zsls.registry;

import org.pbccrc.zsls.config.RegistryConfig;
import org.pbccrc.zsls.registry.zookeeper.ZkMasterRegistry;

public class RegistryFactory {
	
	public static MasterRegistry getMasterRegistry(RegistryConfig config) {
		String addr = config.getConnAddr();
		if (addr == null)
			throw new IllegalArgumentException("null registry address");
		if (config.getProtocol().equals("zookeeper")) {
			try {
				return new ZkMasterRegistry(config);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}

}
