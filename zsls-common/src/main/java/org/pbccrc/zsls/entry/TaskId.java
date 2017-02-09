package org.pbccrc.zsls.entry;

public class TaskId {
	
	public String id;
	
	public TaskId(String id) {
		this.id = id;
	}
	
	public String toString() {
		return id;
	}
	
	public boolean equals(Object o) {
		if (o == null || !(o instanceof TaskId))
			return false;
		if (o == this)
			return true;
		TaskId id = (TaskId)o;
		return id.id.equals(this.id);
	}
	
	public int hashCode() {
		return id.hashCode();
	}

}
