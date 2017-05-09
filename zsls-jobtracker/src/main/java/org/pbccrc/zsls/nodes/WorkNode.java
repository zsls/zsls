package org.pbccrc.zsls.nodes;

import java.util.HashSet;
import java.util.Set;

import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.entry.TaskId;

public class WorkNode {
	
	private NodeId nodeId;
	
	private String domain;
	
	private int maxTaskNum;
	
	private NodeState state;	// useless for now
	
	private Set<TaskId> runningTasks;
	
	// 是否进行过注册。用于主备切换时，标记接收Worker节点的运行时信息。
	private boolean registered;
	
	// 是否被手动置为失效。用于人工干预节点状态，比如单台机器升级，故障处理等。
	private boolean disabled;
	
	// 用于校验节点汇报的执行中任务与服务器端所持有的节点执行任务。
	// 节点汇报运行中的任务有两种情况：1.心跳；2.任务完成反馈。因此汇报间隔上限较高，下限为1个心跳。
	private long lastCheckTaskTime;
	
	
	public WorkNode(String domain, NodeId id, AppContext context, int maxTaskNum, boolean valid) {
		this.domain = domain;
		this.nodeId = id;
		this.maxTaskNum = maxTaskNum;
		this.state = NodeState.NORMAL;
		this.runningTasks = new HashSet<TaskId>();
		this.registered = valid;
	}
	
	public WorkNode(String domain, NodeId id, AppContext context, int maxTaskNum) {
		this.domain = domain;
		this.nodeId = id;
		this.maxTaskNum = maxTaskNum;
		this.state = NodeState.NORMAL;
		this.runningTasks = new HashSet<TaskId>();
		this.registered = true;
	}

	public boolean removeTask(TaskId id) {
		return runningTasks.remove(id);
	}
	
	public void addTask(TaskId task) {
		runningTasks.add(task);
	}
	
	public NodeId getNodeId() {
		return nodeId;
	}
	
	public String getDomain() {
		return domain;
	}
	
	public int getMaxTaskNum() {
		return maxTaskNum;
	}
	
	public void updateMeta(int maxTaskNum) {
		this.maxTaskNum = maxTaskNum;
	}
	
	public Set<TaskId> getRunningTasks() {
		return runningTasks;
	}
	
	public boolean isRunningTask(TaskId task) {
		return runningTasks.contains(task);
	}
	public boolean siRunningTask(String task) {
		return runningTasks.contains(new TaskId(task));
	}
	
	public NodeState getNodeState() {
		return state;
	}

	public boolean isRegistered() {
		return registered;
	}

	public void setRegistered(boolean updated) {
		this.registered = updated;
	}
	
	public boolean isFullyOccupied() {
		return runningTasks.size() >= maxTaskNum;
	}
	
	public boolean isDisabled() {
		return disabled;
	}

	public void setDisabled(boolean disabled) {
		this.disabled = disabled;
	}

	public long getLastCheckTaskTime() {
		return lastCheckTaskTime;
	}

	public void setLastCheckTaskTime(long lastCheckTaskTime) {
		this.lastCheckTaskTime = lastCheckTaskTime;
	}

	public String toString() {
		return "(" + domain + ", " + nodeId + ", " + maxTaskNum + ")";
	}

}
