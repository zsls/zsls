package org.pbccrc.zsls.rpc;

import java.net.InetSocketAddress;

import org.apache.thrift.TServiceClient;
import org.pbccrc.zsls.rpc.client.RpcClientFactory;
import org.pbccrc.zsls.rpc.client.ThrRpcClientFactory;
import org.pbccrc.zsls.rpc.server.Server;
import org.pbccrc.zsls.rpc.server.factory.RpcServerFactory;
import org.pbccrc.zsls.rpc.server.factory.ThrRpcServerFactory;

public class ZuesRPC {
	public static final int DEFAULT_IO_THREADS		= 8;
	public static final int DEFAULT_WORKER_THREADS	= 8;
	
	private static RpcServerFactory serverfactory = new ThrRpcServerFactory();
	
	public static Server getRpcServer(Class<?> protocol, Object instance, 
			InetSocketAddress addr, int workerThreads) {
		return serverfactory.getServer(protocol, instance, addr, DEFAULT_IO_THREADS, workerThreads);
	}
	
	public static Server getRpcServer(Class<?> protocol, Object instance, 
			InetSocketAddress addr, int ioThreads, int workerThreads) {
		return serverfactory.getServer(protocol, instance, addr, ioThreads, workerThreads);
	}
	
	
	private static RpcClientFactory clientFactory = new ThrRpcClientFactory();
	
	public static <T> T getRpcClient(Class<T> protocol, InetSocketAddress addr) throws Exception {
		return clientFactory.getClient(protocol, addr);
	}
	
	public static void closeClient(Object client) {
		try {
			((TServiceClient)client).getInputProtocol().getTransport().close();
		} catch (Exception ignore) {
		}
	}

}
