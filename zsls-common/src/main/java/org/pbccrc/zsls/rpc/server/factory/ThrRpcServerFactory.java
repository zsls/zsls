package org.pbccrc.zsls.rpc.server.factory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.pbccrc.zsls.rpc.server.Server;
import org.pbccrc.zsls.rpc.server.ThrServer;

public class ThrRpcServerFactory implements RpcServerFactory {
	private static final String SURFIX = "$Iface";
	private static final String PROCESS = "$Processor";

	@Override
	public Server getServer(Class<?> protocol, Object instance, InetSocketAddress addr, 
			int ioThreads, int workerThreads) {
		Class<?> proClass = getProcessorClass(protocol);
		Object processor = null;
		if (proClass != null) {
			try {
				Constructor<?> constructor = proClass.getConstructor(protocol);
				processor = constructor.newInstance(instance);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			}
		}
		if (processor != null) {
			return createServer(addr, processor, ioThreads, workerThreads);
		}
		return null;
	}
	
	private Class<?> getProcessorClass(Class<?> protocol) {
		String protocolName = protocol.getName()
				.substring(0, protocol.getName().length() - SURFIX.length());
		String processName = protocolName + PROCESS;
		try {
			return Class.forName(processName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public Server createServer(InetSocketAddress addr, Object processor, int ioThreads, int workerThreads) {
		TNonblockingServerSocket serverTransport;
		try {
			serverTransport = new TNonblockingServerSocket(addr.getPort());
		} catch (TTransportException e) {
			e.printStackTrace();
			return null;
		}
		TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);  
		tArgs.processor((TProcessor)processor);
		tArgs.selectorThreads(ioThreads);
		tArgs.workerThreads(workerThreads);
		tArgs.transportFactory(new TFramedTransport.Factory());  		// 异步
		tArgs.protocolFactory(new TBinaryProtocol.Factory());  			// 二进制协议
		tArgs.maxReadBufferBytes = 10240;
		TServer server = new TThreadedSelectorServer(tArgs);  
		return new ThrServer(server, addr.getPort(), ioThreads);
	}

}
