package org.pbccrc.zsls.jobstore.zookeeper;

public class TaskNode {

	private boolean isDone = false;

	public boolean isDone() {
		return isDone;
	}

	public void setDone(boolean isDone) {
		this.isDone = isDone;
	}
}
