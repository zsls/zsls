package org.pbccrc.zsls.nodes.store;

import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.nodes.NodeState;
import org.pbccrc.zsls.nodes.WorkNode;

public class NodeMeta {
	
	public NodeId nodeId;
	
	public String domain;
	
	public int maxTaskNum;
	
	public NodeState state;
	
	public NodeMeta() {
		
	}
	
	public NodeMeta(String domain, NodeId id, int maxTaskNum, NodeState state) {
		this.domain = domain;
		this.nodeId = id;
		this.maxTaskNum = maxTaskNum;
		this.state = state;
	}
	public NodeMeta(String domain, NodeId id, int maxTaskNum) {
		this.domain = domain;
		this.nodeId = id;
		this.maxTaskNum = maxTaskNum;
		this.state = NodeState.NORMAL;
	}
	
	public static NodeMeta parse(WorkNode node) {
		return new NodeMeta(node.getDomain(), node.getNodeId(), 
				node.getMaxTaskNum(), node.getNodeState());
	}
	
	public String toString() {
		return "domain: " + domain + ", nodeid: " + nodeId;
	}

}
