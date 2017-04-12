package org.pbccrc.zsls.entry;

import java.util.Date;
import java.util.Map;

import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.entry.NodeId.FakeUserNodeId;

public class TaskResult {
	
	TaskAction action;
	
	TaskId taskId;
	
	NodeId nodeId;
	
	Date date;
	
	String appendInfo;
	
	String keyMessage;
	
	Map<String, String> runtimeParams;
	
	public static TaskResult fakeCompleteTaskResult(String taskId) {
		TaskResult ret = new TaskResult();
		ret.action = TaskAction.COMPLETE;
		ret.taskId = new TaskId(taskId);
		ret.nodeId = new FakeUserNodeId();
		return ret;
	}
	
	public TaskAction getAction() {
		return action;
	}

	public void setAction(TaskAction action) {
		this.action = action;
	}

	public TaskId getTaskId() {
		return taskId;
	}

	public void setTaskId(TaskId taskId) {
		this.taskId = taskId;
	}

	public NodeId getNodeId() {
		return nodeId;
	}

	public void setNodeId(NodeId nodeId) {
		this.nodeId = nodeId;
	}

	public String getAppendInfo() {
		return appendInfo;
	}

	public void setAppendInfo(String appendInfo) {
		this.appendInfo = appendInfo;
	}

	public long getRunningTime() {
		return 0L;
	}

	public String getKeyMessage() {
		return keyMessage;
	}

	public void setKeyMessage(String keyMessage) {
		this.keyMessage = keyMessage;
	}

	public Map<String, String> getRuntimeParam() {
		return runtimeParams;
	}

	public void setRuntimeMeta(Map<String, String> runtimeMeta) {
		this.runtimeParams = runtimeMeta;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

}
