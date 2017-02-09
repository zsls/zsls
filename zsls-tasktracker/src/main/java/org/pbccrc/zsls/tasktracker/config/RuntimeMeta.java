package org.pbccrc.zsls.tasktracker.config;

/* meta receive from server */
public class RuntimeMeta {
	
	// heart beat interval
	public long heartBeatInterval;
	
	// how long server would be treated as dead after leaving registry.
	// it's a jobtracker config, but the same value is used here to determine whether
	// the jobtracker is available and it's the time to connect to a standby server.
	public long serverRegistryTimeout;
	
	public RuntimeMeta(long heartBeatInterval, long serverRegistrytimeout) {
		this.heartBeatInterval = heartBeatInterval;
		this.serverRegistryTimeout = serverRegistrytimeout;
	}
	
	public String toString() {
		return "[" + this.heartBeatInterval + ", " + this.serverRegistryTimeout + "]";
	}

}
