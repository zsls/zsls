package org.pbccrc.zsls.sync;

public class Future {
	
	protected volatile boolean complete;
	
	public boolean isDone() {
		return complete;
	}
	
	public void done() {
		synchronized (this) {
			complete = true;
			this.notify();
		}
	}
	
	public void waitForComplete() throws InterruptedException {
		if (complete)
			return;
		synchronized (this) {
			if (!complete)
				this.wait();
		}
	}
	
	public boolean waitForComplete(long timeout) throws InterruptedException {
		if (complete)
			return true;
		synchronized (this) {
			if (!complete)
				this.wait(timeout);
		}
		return complete;
	}

}
