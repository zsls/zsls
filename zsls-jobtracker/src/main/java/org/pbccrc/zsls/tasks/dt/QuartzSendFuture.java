package org.pbccrc.zsls.tasks.dt;

import org.pbccrc.zsls.sync.Future;

public class QuartzSendFuture {
	
	private QuartzTaskInfo taskInfo;
	
	private Future future;
	
	public QuartzSendFuture(QuartzTaskInfo task, Future future) {
		this.taskInfo = task;
		this.future = future;
	}

	public QuartzTaskInfo getTaskInfo() {
		return taskInfo;
	}

	public Future getFuture() {
		return future;
	}
	
}
