package org.pbccrc.zsls.tasktracker.taskhandle;

import java.util.Map;

import org.pbccrc.zsls.api.thrift.records.TTaskInfo;
import org.pbccrc.zsls.api.thrift.records.TaskType;

public class TaskDetail {
	
	Map<String, String> params;
	
	boolean resubmit;
	
	String taskId;
	
	long jobTime;
	
	TaskType type;
	
	public TaskDetail(TaskType type, TTaskInfo info) {
		this.type = type;
		this.params = info.getData();
		this.taskId = info.getTaskid().getTaskid();
		this.jobTime = info.getGenerateTime();
	}

	public String getTaskId() {
		return taskId;
	}

	public boolean isResubmit() {
		return resubmit;
	}

	public void setResubmit(boolean resubmit) {
		this.resubmit = resubmit;
	}

	public Map<String, String> getParams() {
		return params;
	}

	public TaskType getType() {
		return type;
	}

	public long getJobTime() {
		return jobTime;
	}

}
