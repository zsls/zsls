package org.pbccrc.zsls.sched;

import org.pbccrc.zsls.service.AbstractService;
import org.pbccrc.zsls.tasks.dt.QuartzExecutableJob;
import org.pbccrc.zsls.tasks.dt.QuartzTaskManager;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;

public class QuartzScheduler extends AbstractService {
	public static final String GROUP_ZSLS = "zsls_group";
	
	private Scheduler scheduler;
	
	private volatile boolean initLoaded;
	
	public QuartzScheduler() throws SchedulerException {
		super(QuartzScheduler.class.getSimpleName());
		SchedulerFactory schedFactory = new org.quartz.impl.StdSchedulerFactory();
		scheduler = schedFactory.getScheduler();
	}
	
	protected void serviceStart() throws Exception {
		scheduler.start();
		super.serviceStart();
	}
	
	public synchronized void scheduleJobs() throws SchedulerException {
		if (initLoaded)
			return;
		for (ServerQuartzJob job : QuartzTaskManager.getInstance().getJobs())
			scheduleJob(job);
		initLoaded = true;
	}
	
	public synchronized void scheduleJob(ServerQuartzJob sjob) throws SchedulerException {
//		System.out.println("sjob " + sjob.getJobId() + ", stat " + sjob.getJobStat());
		Trigger trigger = sjob.getTrigger();
		JobDetail jobdetail = JobBuilder.newJob(QuartzExecutableJob.class)
				.withIdentity(sjob.getJobId(), GROUP_ZSLS)
				.build();
		if (sjob.getJobStat() != QJobStat.Cancel)
			scheduler.scheduleJob(jobdetail, trigger);
	}
	
	public synchronized boolean cancelJob(ServerQuartzJob sjob) throws SchedulerException {
		JobKey key = new JobKey(sjob.getJobId(), GROUP_ZSLS);
		scheduler.pauseJob(key);
		return true;
	}
	
	public synchronized boolean resumeJob(ServerQuartzJob sjob) throws SchedulerException {
		JobKey key = new JobKey(sjob.getJobId(), GROUP_ZSLS);
		QuartzTaskManager.getInstance().putJob(sjob.getJobId(), sjob);
		JobDetail jobDetail = scheduler.getJobDetail(key);
		if (jobDetail != null)
			scheduler.resumeJob(key);
		else {
			jobDetail = JobBuilder.newJob(QuartzExecutableJob.class)
				.withIdentity(sjob.getJobId(), GROUP_ZSLS)
				.build();
			scheduler.scheduleJob(jobDetail, sjob.getTrigger());
		}
		return true;
	}

}
