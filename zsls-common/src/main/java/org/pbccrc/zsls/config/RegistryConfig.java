package org.pbccrc.zsls.config;

public class RegistryConfig {
	
	public static final String REGIST_PREFIX			= "registry";
	public static final String REGIST_ADDR			= "addr";
	public static final String REGIST_CONN_TIMEOUT	= "conn.timeout";
	public static final String REGIST_SESS_TIMEOUT	= "session.timeout";
	
	public static RegistryConfig readConfig(Configuration conf) {
		String addr = conf.get(REGIST_PREFIX + "." + REGIST_ADDR);
		long connTimeout = conf.getLong(REGIST_PREFIX + "." + REGIST_CONN_TIMEOUT,  1000L);
		long sessTimeout = conf.getLong(REGIST_PREFIX + "." + REGIST_SESS_TIMEOUT,  
				ZslsConstants.DEFAULT_NODE_LOST);
		return new RegistryConfig(addr, connTimeout, sessTimeout);
	}
	
	public RegistryConfig (String addr, long connTimeout, long sessTimeout) {
		parseAddr(addr);
		this.connTimeout = connTimeout;
		this.sessionTimeout = sessTimeout;
	}
	
	private String protocol;
	
	private String connAddr;
	
	private long connTimeout;
	
	private long sessionTimeout;

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public String getConnAddr() {
		return connAddr;
	}

	public void setConnAddr(String connAddr) {
		this.connAddr = connAddr;
	}

	public long getConnTimeout() {
		return connTimeout;
	}

	public void setConnTimeout(long connTimeout) {
		this.connTimeout = connTimeout;
	}

	public long getSessionTimeout() {
		return sessionTimeout;
	}

	public void setSessionTimeout(long sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}
	
	private void parseAddr(String addr) {
		int idx = addr.indexOf("://");
		if (idx > 0) {
			protocol = addr.substring(0, idx).trim();
			connAddr = addr.substring(idx + 3).trim();
		}
	}

}
