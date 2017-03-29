package org.pbccrc.zsls.tasks;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.eventdispatch.AsyncDispatcher;
import org.pbccrc.zsls.eventdispatch.Event;
import org.pbccrc.zsls.eventdispatch.EventHandler;
import org.pbccrc.zsls.jobengine.JobFlow.JobStat;
import org.pbccrc.zsls.jobengine.JobId;
import org.pbccrc.zsls.service.AbstractService;
import org.pbccrc.zsls.tasks.dt.DTJobId;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobId;

public class UnitMarker extends AbstractService implements EventHandler<UnitEvent> {
	
	private AsyncDispatcher dispatcher;
	
	private int size;
	
	private AppContext context;
	
	public UnitMarker(AppContext context) {
		super(UnitMarker.class.getSimpleName());
		this.size = 10000;
		this.context = context;
	}
	
	protected void serviceInit(Configuration config) throws Exception {
		@SuppressWarnings("rawtypes")
		BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(size);
		dispatcher = new AsyncDispatcher(queue);
		dispatcher.register(UnitEventType.class, this);
		dispatcher.init(config);
		super.serviceInit(config);
	}
	
	protected void serviceStart() throws Exception {
		dispatcher.start();
		super.serviceStart();
	}
	
	@SuppressWarnings("unchecked")
	public void rtUnitFinish(String domain, JobId id) {
		dispatcher.getEventHandler().handle(new UnitEvent(domain, id, UnitEventType.RTFinished));
	}
	
	@SuppressWarnings("unchecked")
	public void dtUnitFinish(String domain, JobId id) {
		dispatcher.getEventHandler().handle(new UnitEvent(domain, id, UnitEventType.DTFinished));
	}
	
	@SuppressWarnings("unchecked")
	public void unitResubmit(String domain, RTJobId id) {
		dispatcher.getEventHandler().handle(new UnitEvent(domain, id, UnitEventType.Resubmit));
	}

	@Override
	public void handle(UnitEvent event) {
		String domain = event.getDomain();
		switch (event.getType()) {
		case RTFinished:
			RTJobId id = (RTJobId)event.getUnitId();
			context.getJobStore().updateJob(domain, id, JobStat.Finished);
			break;
			
		case DTFinished:
			DTJobId jobId = (DTJobId)event.getUnitId();
			context.getJobStore().updateJobStatus(jobId.toString(), QJobStat.Finish);
			break;
			
		case Resubmit:
			id = (RTJobId)event.getUnitId();
			context.getJobStore().updateJob(domain, id, JobStat.Unfinish);
			break;
		}
	}

}