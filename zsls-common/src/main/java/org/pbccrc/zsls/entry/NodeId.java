package org.pbccrc.zsls.entry;

public class NodeId {
	
	public NodeId(String domain, String name, String ip, int port) {
		this.domain = domain;
		this.name = name;
		this.ip = ip;
		this.port = port;
	}
	
	public NodeId(String domain, String ip, int port) {
		this.domain = domain;
		this.ip = ip;
		this.port = port;
	}
	
	public String domain;
	
	public String name;
	
	public String ip;
	
	// port of TaskHandleProtocol service
	public int port;
	
	public static NodeId getNodeId(String domain, String addr) {
		try {
			String[] splits = addr.split(":");
			String ip = splits[0];
			int port = Integer.parseInt(splits[1]);
			return new NodeId(domain, ip, port);
		} catch (Exception e) {
		}
		return null;
	}
	
	public static class FakeUserNodeId extends NodeId {
		public FakeUserNodeId(String domain, String ip, int port) {
			super(domain, ip, port);
		}
		public FakeUserNodeId() {
			this(null, "", 0);
		}
		public String toString() {
			return "<user command>";
		}
		public boolean isFake() {
			return true;
		}
	}
	
	public boolean isFake() {
		return false;
	}
	
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || !(o instanceof NodeId))
			return false;
		NodeId id = (NodeId)o;
		return ip.equals(id.ip) && port == id.port;
	}
	
	public int hashCode() {
		int ret = 17;
		ret = 37 * ret + ip.hashCode();
		ret = 37 * ret + port;
		return ret;
	}
	
	public String toString() {
		return ip + ":" + port;
	}

}
