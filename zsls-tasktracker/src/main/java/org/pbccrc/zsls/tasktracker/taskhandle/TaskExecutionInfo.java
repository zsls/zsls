package org.pbccrc.zsls.tasktracker.taskhandle;

import java.util.Map;

import org.pbccrc.zsls.utils.Shell;

public class TaskExecutionInfo {
	
	// task specific context
	public TaskContext context;
	
	// log message sent back to server as feedback
	public String feedbackMsg;
	
	// task result that could influent the job flow. 
	public String keyMessage;
	
	// parameters that could be used next time, used for quartz tasks.
	public Map<String, String> runtimeParams;
	
	// for shell task handler
	public Shell shell;
	
	public TaskExecutionInfo(TaskContext context) {
		this.context = context;
	}

}
