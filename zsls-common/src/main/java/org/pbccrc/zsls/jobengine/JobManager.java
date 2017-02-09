package org.pbccrc.zsls.jobengine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JobManager {
	
	private Map<String, Task> taskCache = new ConcurrentHashMap<String, Task>(2048);
	private Map<String, JobFlow> unitCache = new ConcurrentHashMap<String, JobFlow>(512);
	
	public Task getTask(String taskId) {
		return taskCache.get(taskId);
	}
	
	public JobFlow getUnit(String jobId) {
		return unitCache.get(jobId);
	}
	
	public List<String> getRegisteredUnits() {
		List<String> list = new ArrayList<String>();
		for (String d : unitCache.keySet())
			list.add(d);
		return list;
	}
	
	public int getTaskCacheSize() {
		return taskCache.size();
	}
	
	public boolean register(JobFlow unit) {
		if (unitCache.containsKey(unit.getJobId().toString()))
			return false;
		unitCache.put(unit.getJobId().toString(), unit);
		Iterator<Task> it = unit.getTaskIterator();
		while (it.hasNext()) {
			Task task = it.next();
			taskCache.put(task.getTaskId(), task);
		}
		return true;
	}
	
	public void unregister(JobFlow unit) {
		unregister(unit.getJobId().toString());
	}
	
	public void unregister(String jobId) {
		JobFlow unit = unitCache.remove(jobId);
		if (unit == null)
			return;
		Iterator<Task> it = unit.getTaskIterator();
		while (it.hasNext()) {
			Task task = it.next();
			taskCache.remove(task.getTaskId());
		}
	}

}
