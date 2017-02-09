package org.pbccrc.zsls.registry;

/**
 * 注册节点，系统中为Jobtracker
 * 
 */
public class RegNode {
	
	String name;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	String ip;
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	
	// innerTracker端口
	int port;
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	// 对外的http端口
	int httpPort;
	public int getHttpPort() {
		return httpPort;
	}
	public void setHttpPort(int httpPort) {
		this.httpPort = httpPort;
	}
	
	public String toString() {
		return ip + ":" + port + ":" + httpPort;
	}
	
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || !(o instanceof RegNode))
			return false;
		RegNode node = (RegNode)o;
		return node.ip.equals(ip) && node.port == port && node.httpPort == httpPort;
	}
	
	public int hashCode() {
		int ret = 17;
		ret = ret * 37 + ip.hashCode();
		ret = ret * 37 + port;
		ret = ret * 37 + httpPort;
		return ret;
	}

}
