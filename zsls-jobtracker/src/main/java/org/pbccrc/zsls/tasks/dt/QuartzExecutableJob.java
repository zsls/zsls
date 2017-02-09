package org.pbccrc.zsls.tasks.dt;

import java.util.Date;

import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.JobManager;
import org.pbccrc.zsls.sync.Future;
import org.pbccrc.zsls.tasks.LocalJobManager;
import org.pbccrc.zsls.tasks.TaskEvent;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.pbccrc.zsls.utils.DomainLogger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class QuartzExecutableJob implements Job {
	public static final int WAIT_TIME = 5000;
	
	private static DomainLogger L = DomainLogger.getLogger(QuartzExecutableJob.class.getSimpleName());
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String jobId = context.getJobDetail().getKey().getName();
		ServerQuartzJob job = QuartzTaskManager.getInstance().getJob(jobId);
		if (job == null)
			throw new JobExecutionException("cannot find quartz job " + jobId + " in task manager");
		
		switch (job.getJobStat()) {
		case Finish:
		case Init:
			break;
			
		case Run:
			int triggerLevel = job.getTriggerCondition();
			if (triggerLevel >= ServerQuartzJob.TRIGGER_LVL_FORCE)
				L.error(ZslsConstants.FAKE_DOMAIN_DT, "Quartz trigger level with " + triggerLevel
						+ "not supported, abort launching job " + job.getJobId() + ", curstate: Run");
			else
				L.error(ZslsConstants.FAKE_DOMAIN_DT, "Quartz job " + job.getJobId() + " still running");
			return;
				
		case Stuck:
			triggerLevel = job.getTriggerCondition();
			if (triggerLevel >= ServerQuartzJob.TRIGGER_LVL_STUCK) {
				L.info(ZslsConstants.DEFAULT_DOMAIN, "force refresh stucked job " + jobId + ", last feed time: " + 
							job.getLastExecuteTime());
				forceJobFinished(job);
			}
			break;
			
		case Cancel:
			L.warn(ZslsConstants.FAKE_DOMAIN_DT, "QuartzJob " + jobId + " has been canceled");
			return;
		}
		
		Future future = addSend(job);
		if (future == null)
			return;
		try {
			L.info(ZslsConstants.FAKE_DOMAIN_DT, "fire quartz job " + jobId);
			future.waitForComplete(WAIT_TIME);
		} catch (InterruptedException ignore) {
		}
		if (!future.isDone()) {
			L.error(ZslsConstants.FAKE_DOMAIN_DT, "quartz job " + jobId + " send future timeout");
		}
	}
	
	private void forceJobFinished(ServerQuartzJob job) {
		job.changeStatus(QJobStat.Finish);
		JobManager manager = LocalJobManager.getDTJobManager();
		manager.unregister(job.getJobId());
	}
	
	@SuppressWarnings("unchecked")
	private Future addSend(ServerQuartzJob job) {
		Future future = new Future();
		JobFlow newJob = job.generateJobInstance();
		if (newJob == null) {
			L.error(ZslsConstants.FAKE_DOMAIN_DT, "failed to generate Job instance " + job.getJobId());
			return null;
		}
		
		AppContext context = QuartzTaskManager.getInstance().getContext();
		Date date = new Date();
		newJob.setGenerateTime(date);
		if (!context.getJobStore().addJobFlowInstance(job, date)) {
			L.error(ZslsConstants.FAKE_DOMAIN_DT, "failed to generate job instance in JobStore");
			return null;
		}
		
		QuartzTaskInfo task = new QuartzTaskInfo(newJob, future, date);
		TaskEvent event = TaskEvent.getDTNewJobEvent(task);
		context.getTaskDispatcher().getEventHandler().handle(event);
		return future;
	}
	
}
