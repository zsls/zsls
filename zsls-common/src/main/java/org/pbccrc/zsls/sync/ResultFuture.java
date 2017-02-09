package org.pbccrc.zsls.sync;

public class ResultFuture<T> {
	
	protected T result;
	
	public T getResult() {
		return result;
	}
	
	public void setResult(T result) {
		synchronized (this) {
			this.result = result;
			this.notify();
		}
	}
	
	public T waitAndGetResult() throws InterruptedException {
		if (result != null)
			return result;
		synchronized (this) {
			if (result == null) {
				this.wait();
			}
		}
		return result;
	}
	
	public T waitAndGetResult(long timeout) throws InterruptedException {
		if (result != null)
			return result;
		synchronized (this) {
			if (result == null) {
				this.wait(timeout);
			}
		}
		return result;
	}

}
