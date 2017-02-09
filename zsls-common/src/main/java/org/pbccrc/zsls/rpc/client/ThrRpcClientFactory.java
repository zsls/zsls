package org.pbccrc.zsls.rpc.client;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ThrRpcClientFactory implements RpcClientFactory {
	
	private static final String SURFIX = "$Iface";
	private static final String CLIENT = "$Client";

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getClient(Class<T> protocol, InetSocketAddress addr) throws Exception {
		if (protocol.getSimpleName().endsWith(SURFIX))
			return null;
		String protocolName = protocol.getName()
				.substring(0, protocol.getName().length() - SURFIX.length());
		String clientName = protocolName + CLIENT;
		Class<? extends T> clientClass = (Class<? extends T>) Class.forName(clientName);
		Constructor<? extends T> constructor = clientClass.getConstructor(TProtocol.class);
		TSocket socket = new TSocket(addr.getHostName(), addr.getPort());
		socket.setTimeout(3000);
		TTransport transport = new TFramedTransport(socket);
		transport.open();
		TProtocol tprotocol = new TBinaryProtocol(transport);
		T ret = constructor.newInstance(tprotocol);
		return ret;
	}
	
}
