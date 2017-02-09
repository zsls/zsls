package org.pbccrc.zsls.nodes;

import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.eventdispatch.AbstractEvent;

public class NodeEvent extends AbstractEvent<NodeEventType> {
	
	private NodeId nodeId;
	
	private String domain;

	private NodeEvent(NodeEventType type) {
		super(type);
	}
	
	public NodeEvent(NodeEventType type, NodeId id, String domain) {
		super(type);
		this.nodeId = id;
		this.domain = domain;
	}
	
	public NodeId getNodeId() {
		return nodeId;
	}
	
	public String getDomain() {
		return domain;
	}
	
	public String toString() {
		return nodeId + ", event(" + this.getType() + ")";
	}
	
}
