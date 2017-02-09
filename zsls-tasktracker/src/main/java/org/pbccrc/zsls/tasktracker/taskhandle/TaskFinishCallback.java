package org.pbccrc.zsls.tasktracker.taskhandle;

public interface TaskFinishCallback {
	
	void onTaskComplte(TaskExecutionInfo info);
	
	void onTaskFail(TaskExecutionInfo info);

}
