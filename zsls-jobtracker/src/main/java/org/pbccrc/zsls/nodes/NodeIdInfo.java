package org.pbccrc.zsls.nodes;

import org.pbccrc.zsls.entry.NodeId;

public class NodeIdInfo {
	
	private NodeId id;
	
	private String domain;
	
	public NodeIdInfo(NodeId id, String domain) {
		this.id = id;
		this.domain = domain;
	}

	public NodeId getId() {
		return id;
	}

	public void setId(NodeId id) {
		this.id = id;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	public boolean equals(Object o) {
		if (o == this)
			return true;
		else if (o == null || !(o instanceof NodeIdInfo))
			return false;
		return id.equals(((NodeIdInfo)o).getId());
	}
	
	public int hashCode() {
		return id.hashCode();
	}

}
