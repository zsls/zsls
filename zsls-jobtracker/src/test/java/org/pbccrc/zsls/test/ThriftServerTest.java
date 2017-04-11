package org.pbccrc.zsls.test;

import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.pbccrc.zsls.api.thrift.TaskHandleProtocol;
import org.pbccrc.zsls.api.thrift.records.TaskHandleRequest;
import org.pbccrc.zsls.rpc.ZuesRPC;
import org.pbccrc.zsls.rpc.server.Server;

public class ThriftServerTest {
	
	public static void main(String[] args) {
		testServer();
	}
	
	public static void testServer() {
		int port = 2609;
		InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);
		Server server = ZuesRPC.getRpcServer(TaskHandleProtocol.Iface.class, 
				new Handler(), addr, 2, 0);
		server.start();
		System.out.println("server start on port " + port);
	}
	
	static class Handler implements TaskHandleProtocol.Iface {
		@Override
		public void assignTask(TaskHandleRequest request) throws TException {
			System.out.println("server receive request");
		}

		@Override
		public void killTask(TaskHandleRequest request) throws TException {
			
		}
	}

}
