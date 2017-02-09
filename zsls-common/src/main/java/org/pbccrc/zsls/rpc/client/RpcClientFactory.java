package org.pbccrc.zsls.rpc.client;

import java.net.InetSocketAddress;

public interface RpcClientFactory {
	
	<T> T getClient(Class<T> protocol, InetSocketAddress addr) throws Exception;

}
