package org.pbccrc.zsls.nodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.Task;

public class WorkerManager {
	
	private ConcurrentHashMap<String, Map<NodeId, WorkNode>> nodes = 
			new ConcurrentHashMap<String, Map<NodeId, WorkNode>>();
	
	public WorkNode getNode(String domain, NodeId id) {
		Map<NodeId, WorkNode> domainNodes = nodes.get(domain);
		if (domainNodes != null)
			return domainNodes.get(id);
		return null;
	}
	
	public Map<NodeId, WorkNode> getNodes(String domain) {
		return nodes.get(domain);
	}
	
	public boolean isDomainEmpty(String domain) {
		return nodes.get(domain) == null || nodes.get(domain).isEmpty();
	}
	
	public void addWorkNode(String domain, WorkNode node) {
		Map<NodeId, WorkNode> domainNodes = nodes.get(domain);
		if (domainNodes == null) {
			Map<NodeId, WorkNode> tmp = new ConcurrentHashMap<NodeId, WorkNode>();
			domainNodes = nodes.putIfAbsent(domain, tmp);
			if (domainNodes == null)
				domainNodes = tmp;
		}
		domainNodes.put(node.getNodeId(), node);
	}
	
	public WorkNode removeWorkNode(String domain, NodeId id) {
		Map<NodeId, WorkNode> domainNodes = nodes.get(domain);
		if (domainNodes != null)
			return domainNodes.remove(id);
		return null;
	}
	
	public void delDomain(String domain) {
		nodes.remove(domain);
	}
	
	Lock addLock = new ReentrantLock();
	public void lockAdd(String domain) {
		addLock.lock();
	}
	public void unlockAdd() {
		addLock.unlock();
	}
	public Object getAddNodeLock() {
		return addLock;
	}
	
	public WorkNode getNodeWithAssignedTask(String domain, TaskId taskId) {
		Map<NodeId, WorkNode> domainNodes = nodes.get(domain);
		if (domainNodes != null) {
			synchronized (domainNodes) {
				for (WorkNode node : domainNodes.values()) {
					if (node.getRunningTasks().contains(taskId))
						return node;
				}	
			}
		}
		return null;
	}
	
	public Map<WorkNode, List<String>> getNodesWithRunningJob(JobFlow job) {
		Map<WorkNode, List<String>> ret = new HashMap<WorkNode, List<String>>();
		HashSet<String> tasks = new HashSet<String>();
		for (Task t : job.getTasks())
			tasks.add(t.getTaskId());
		for (Map<NodeId, WorkNode> nds : nodes.values()) {
			for (WorkNode node : nds.values()) {
				for (TaskId t : node.getRunningTasks()) {
					if (tasks.contains(t.id)) {
						List<String> list = ret.get(node);
						if (list == null) {
							list = new ArrayList<String>();
							ret.put(node, list);
						}
						list.add(t.id);
					}
				}
			}
		}
		return ret;
	}
	
	public boolean isAllRegistered(String domain) {
		Map<NodeId, WorkNode> domainNodes = nodes.get(domain);
		if (domainNodes != null) {
			synchronized (domainNodes) {
			for (WorkNode node : domainNodes.values())
				if (!node.isRegistered())
					return false;	
			}
		}
		return true;
	}
	
	public boolean isAllDomainRegistered() {
		for (String domain : nodes.keySet()) {
			if (!isAllRegistered(domain))
				return false;
		}
		return true;
	}
	
	public List<NodeId> getUnregisteredNodes(String domain) {
		List<NodeId> list = new ArrayList<NodeId>();
		Map<NodeId, WorkNode> domainNodes = nodes.get(domain);
		if (domainNodes != null) {
			synchronized (domainNodes) {
				for (Map.Entry<NodeId, WorkNode> e : domainNodes.entrySet()) {
					if (!e.getValue().isRegistered())
						list.add(e.getKey());
				}
			}
		}
		return list;
	}

}
