package org.pbccrc.zsls.tasktracker.taskhandle;

public interface TaskHandler {
	
	boolean init();
	
	boolean handleTask(TaskContext context);

}
