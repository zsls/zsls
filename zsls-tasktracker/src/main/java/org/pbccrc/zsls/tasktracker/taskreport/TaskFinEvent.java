package org.pbccrc.zsls.tasktracker.taskreport;

import org.pbccrc.zsls.eventdispatch.AbstractEvent;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskExecutionInfo;

public class TaskFinEvent extends AbstractEvent<TaskFinType> {
	
	private TaskExecutionInfo info;

	public TaskFinEvent(TaskFinType type, TaskExecutionInfo info) {
		super(type);
		this.info = info;
	}
	
	public TaskExecutionInfo getContext() {
		return info;
	}

}
