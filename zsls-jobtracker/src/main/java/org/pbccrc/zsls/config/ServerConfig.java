package org.pbccrc.zsls.config;

public class ServerConfig {
	public static final String NAME_FRONT	= "front";
	public static final String NAME_TRACK	= "tracker";
	
	public static final String PREFIX	= "server";
	public static final String PORT	= "port";
	public static final String THREAD	= "iothreads";
	public static final String WORKTHREAD	= "workerthreads";
	
	public static ServerConfig readConfig(Configuration conf, String name) {
		ServerConfig config = new ServerConfig();
		config.name = name;
		config.port = conf.getInt(PREFIX + "." + name + "." + PORT);
		config.ioThreads = conf.getInt(PREFIX + "." + name + "." + THREAD);
		config.workerThreads = conf.getInt(PREFIX + "." + name + "." + WORKTHREAD);
		if (config.port == Configuration.NULL_INT_VAL ||
				config.ioThreads == Configuration.NULL_INT_VAL ||
				config.workerThreads == Configuration.NULL_INT_VAL)
			return null;
		return config;
	}
	
	private int port;
	public int getPort() {
		return port;
	}
	
	private int workerThreads;
	public int getWorkerThreads() {
		return workerThreads;
	}
	
	private int ioThreads;	
	public int getIoThreads() {
		return ioThreads;
	}
	
	private String name;
	public String getName() {
		return name;
	}

}
