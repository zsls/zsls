package org.pbccrc.zsls.tasks.rt;

import java.util.Iterator;

import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.Task;

public class RTJobFlow extends JobFlow {

	private RTJobId preUnit;
	public RTJobId getPreUnit() {
		return preUnit;
	}
	public void setPreUnit(RTJobId preUnit) {
		this.preUnit = preUnit;
	}
	
	public RTJobId getJobId() {
		return (RTJobId)jobId;
	}
	
	public void setUnitId(long id) {
		jobId = new RTJobId(id);
	}
	
	public void updateUnitIdForAllTasks(long unitId) {
		jobId = new RTJobId(unitId);
		Iterator<Task> it = this.getTaskIterator();
		while (it.hasNext()) {
			RTTask task = (RTTask)it.next();
			task.updateTaskId(jobId.toString());
		}
	}

}
