package org.pbccrc.zsls.rpc.server;

import org.apache.thrift.server.TServer;

public class ThrServer implements Server {
	
	TServer server;
	
	int port;
	
	int readerNum;
	
	Thread thread;
	
	public ThrServer(TServer server, int port, int readerNum) {
		this.server = server;
		this.port = port;
		this.readerNum = readerNum;
	}

	@Override
	public void start() {
		thread = new Thread() {
			public void run() {
				server.serve();
			}
		};
		thread.start();
	}

	@Override
	public void stop() {
		server.stop();
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public int getNumReaders() {
		return readerNum;
	}

}
