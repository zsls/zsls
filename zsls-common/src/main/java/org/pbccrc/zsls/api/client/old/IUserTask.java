package org.pbccrc.zsls.api.client.old;

import java.util.HashMap;
import java.util.Map;

public class IUserTask {
	
	public String id;
	
	public long timeout;
	
	public double priority;
	
	public Map<String, String> params;
	
	public String retryOp;
	
	public IUserTask() {
	}
	
	public IUserTask(String id) {
		this.id = id;
		this.params = new HashMap<String, String>();
	}
	
	public double getPriority() {
		return this.priority;
	}
	
	public void setPriority(double priority) {
		this.priority = priority;
	}
	
	public int hashCode() {
		return id.hashCode();
	}
	
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof IUserTask))
			return false;
		IUserTask task = (IUserTask)obj;
		return task.id != null && task.id.equals(id);
	}
}
