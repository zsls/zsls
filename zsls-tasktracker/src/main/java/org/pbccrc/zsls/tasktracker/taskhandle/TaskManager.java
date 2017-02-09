package org.pbccrc.zsls.tasktracker.taskhandle;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskManager {
	
	private Map<String, TaskExecutionInfo> data =
			new ConcurrentHashMap<String, TaskExecutionInfo>();
	
	public boolean registerTask(String taskId, TaskContext context) {
		if (data.containsKey(taskId))
			return false;
		data.put(taskId, new TaskExecutionInfo(context));
		return true;
	}
	
	public void unregisterTask(String taskId) {
		data.remove(taskId);
	}
	
	public TaskExecutionInfo getTaskExecutionInfo(String taskId) {
		return data.get(taskId);
	}
	
	public Collection<TaskExecutionInfo> getTaskExecutionInfoSet() {
		return data.values();
	}

}
