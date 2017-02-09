package org.pbccrc.zsls.utils;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.ServerConfig;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.registry.RegNode;

public class RegisterUtils {
	
	public static RegNode getLocalRegNode(AppContext context) {
		RegNode node = new RegNode();
		Configuration config = context.getConfig();
		node.setIp(LocalUtils.getLocalIp());
		node.setHttpPort(config.getInt(ServerConfig.PREFIX + "." + ServerConfig.NAME_FRONT + "." +
				ServerConfig.PORT));
		node.setPort(config.getInt(ServerConfig.PREFIX + "." + ServerConfig.NAME_TRACK+ "." +
				ServerConfig.PORT));
		return node;
	}

}
