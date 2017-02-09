package org.pbccrc.zsls.entry;

import java.util.Map;

public class TaskInfo {
	
	private TaskId taskId;
	
	private Map<String, String> data;
	
	private long generateTime;

	public TaskId getTaskId() {
		return taskId;
	}

	public void setTaskId(TaskId taskId) {
		this.taskId = taskId;
	}

	public Map<String, String> getData() {
		return data;
	}

	public void setData(Map<String, String> data) {
		this.data = data;
	}

	public long getGenerateTime() {
		return generateTime;
	}

	public void setGenerateTime(long generateTime) {
		this.generateTime = generateTime;
	}
	
}