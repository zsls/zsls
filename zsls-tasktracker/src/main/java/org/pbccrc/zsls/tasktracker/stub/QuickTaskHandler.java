package org.pbccrc.zsls.tasktracker.stub;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskContext;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskHandler;

public class QuickTaskHandler implements TaskHandler {
	
	protected static Logger L = Logger.getLogger(StubTaskHandler.class.getSimpleName());

	@Override
	public boolean init() {
		return true;
	}

	@Override
	public boolean handleTask(TaskContext context) {
		return true;
	}

}
