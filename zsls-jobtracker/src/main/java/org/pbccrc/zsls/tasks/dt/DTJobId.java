package org.pbccrc.zsls.tasks.dt;

import org.pbccrc.zsls.jobengine.JobId;

public class DTJobId extends JobId {
	
	private String id;
	
	public DTJobId(String id) {
		this.id = id;
	}
	
	public String toString() {
		return id;
	}

}
