package org.pbccrc.zsls.tasks.dt;

import java.util.Date;

import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.sync.Future;

public class QuartzTaskInfo {
	
	private JobFlow job;
	
	private Future future;
	
	public QuartzTaskInfo(JobFlow job, Future future, Date date) {
		this.job = job;
		this.future = future;
		this.job.setGenerateTime(date);
	}

	public JobFlow getJob() {
		return job;
	}

	public Future getFuture() {
		return future;
	}
	
}
