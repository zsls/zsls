package org.pbccrc.zsls.rpc.server;

public interface Server {
	
	void start();
	
	void stop();
	
	int getPort();
	
	int getNumReaders();

}
