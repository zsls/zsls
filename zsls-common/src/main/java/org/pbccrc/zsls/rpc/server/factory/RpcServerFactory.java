package org.pbccrc.zsls.rpc.server.factory;

import java.net.InetSocketAddress;

import org.pbccrc.zsls.rpc.server.Server;

public interface RpcServerFactory {
	
	Server getServer(Class<?> protocol, Object instance,
			InetSocketAddress addr, int ioThreads, int workerThreads);
	
}
