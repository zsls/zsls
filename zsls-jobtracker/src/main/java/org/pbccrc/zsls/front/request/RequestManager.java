package org.pbccrc.zsls.front.request;

import java.util.ArrayList;
import java.util.List;

import org.pbccrc.zsls.JobTracker.DomainStatus;
import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.old.UnitValidator;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.api.thrift.records.TTaskId;
import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.domain.DomainManager;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.eventdispatch.AsyncDispatcher;
import org.pbccrc.zsls.eventdispatch.Dispatcher;
import org.pbccrc.zsls.eventdispatch.EventHandler;
import org.pbccrc.zsls.front.request.QRequest.Status;
import org.pbccrc.zsls.front.request.UserRequest.JobType;
import org.pbccrc.zsls.front.request.UserRequest.QueryType;
import org.pbccrc.zsls.front.request.utils.ParamValidator;
import org.pbccrc.zsls.front.request.utils.QueryHelper;
import org.pbccrc.zsls.front.request.utils.Replyer;
import org.pbccrc.zsls.front.request.utils.ValidResult;
import org.pbccrc.zsls.front.result.Result;
import org.pbccrc.zsls.ha.HAStatus;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.jobstore.JobStore;
import org.pbccrc.zsls.nodes.WorkNode;
import org.pbccrc.zsls.nodes.WorkerManager;
import org.pbccrc.zsls.registry.RegNode;
import org.pbccrc.zsls.service.CompositeService;
import org.pbccrc.zsls.tasks.LocalJobManager;
import org.pbccrc.zsls.tasks.TaskEvent;
import org.pbccrc.zsls.tasks.dt.QuartzTaskManager;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.JsonSerilizer;
import org.pbccrc.zsls.utils.TaskUtil;
import org.quartz.SchedulerException;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

public class RequestManager extends CompositeService implements 
		FrontServerCallback<FullHttpRequest, FullHttpResponse>, 
		EventHandler<UserRequestEvent> {

	private static DomainLogger L = DomainLogger.getLogger(RequestManager.class.getSimpleName());
	
	/* asynchronous dispatcher used to register requests */
	protected Dispatcher dispatcher;
	
	/* context */
	protected AppContext context;
	
	protected QueryHelper queryHelper;
	
	public RequestManager(AppContext context) {
		super(RequestManager.class.getSimpleName());
		this.context = context;
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		this.dispatcher = new AsyncDispatcher();
		dispatcher.register(QueryType.class, this);
		addIfService(dispatcher);
		this.queryHelper = new QueryHelper(context);
		super.serviceInit(conf);
	}
	
	protected void serviceStart() throws Exception {
		super.serviceStart();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int callback(FullHttpRequest httpRequest, Replyable<FullHttpResponse> reply) {
		UserRequest userRequest = UserRequest.parseFromHttpRequest(httpRequest);
		QRequest request = (QRequest)reply;
		request.setUserRequest(userRequest);
		request.setServerStatus(HAStatus.MASTER);
		L.info(userRequest.getDomain(), userRequest.toString());
		
		// validate parameters
		ValidResult ret = ParamValidator.validateRequest(request, context);
		if (!ret.valid) {
			Replyer.replyAbnormal(request, Status.Invalid, ret.info);
			return -1;
		}
		
		// HA status
		if (context.getHAStatus() == HAStatus.STANDBY) {
			request.setServerStatus(HAStatus.STANDBY);
			RegNode node = context.getRegistry().getMaster();
			String masterAddr = node != null ? (node.getIp() + ":" + node.getHttpPort()) : null;
			request.setMasterAddress(masterAddr);
			Replyer.replyAbnormal(request, Status.Fail, " receive request while standing by, reply master: " + masterAddr);
			return -1;
		} 
		else if (context.getHAStatus() == HAStatus.UNKNOWN) {
			request.setServerStatus(HAStatus.UNKNOWN);
			Replyer.replyAbnormal(request, Status.Fail, "unknown master status");
			return -1;
		}

		// start process requests
		switch (userRequest.getQueryType()) {
		case DisableNodeQuery:
			disableNode(request);
			break;
			
		case CronQuartzJob:
		case SimpleQuartzJob:
			processQuartzJob(request);
			break;
		case SchedCMDQuery:
			processCMDQuery(request);
			break;
		
		case SchedStatQuery:
			processStatQuery(request);
			break;
			
		case SchedRedoTask:
			processRedoRequest(request);
			break;
		case QuartzCmd:
			processQuartzCmdRequest(request);
			break;
		case SchedMarkTask:
			processMarkRequest(request);
			break;
			
		case ScheduleRequest:
			UserRequestEvent event = new UserRequestEvent(request);
			dispatcher.getEventHandler().handle(event);
			return 0;
			
		default:
			break;
		}
		return 0;
	}
	
	@SuppressWarnings("unchecked")
	protected void processQuartzCmdRequest(QRequest request) {
		UserRequest userRequest = request.getUserRequest();
		String jobId = userRequest.getQuery();
		ServerQuartzJob sjob = QuartzTaskManager.getInstance().getJob(jobId);
		String cmd = userRequest.getCmd();
		if (sjob == null) {
			Replyer.replyAbnormal(request, Status.Fail, "cannot find quartz job with id " + jobId);
			return;
		}
		TaskEvent event = null;
		String msg = null;
		if (UserRequest.PARAM_VAL_CANCEL.equals(cmd)) {
			if (!context.getJobStore().updateJobStatus(jobId, QJobStat.Cancel)) {
				Replyer.replyAbnormal(request, Status.Fail, "failed to cancel job " + jobId + " in jobstore");
				return;
			}
			try {
				context.getQuartzScheduler().cancelJob(sjob);
			} catch (SchedulerException e) {
				// should not happen, since we use RAMJobStore for quartz.
				e.printStackTrace();
			}
			event = TaskEvent.getKillJobEvent(DomainType.DT, jobId);
			msg = "quartz job " + jobId + " canceled successfully";
		}
		else {
			if (sjob.getJobStat() != QJobStat.Cancel)
				msg = "quartz job " + jobId + " not canceled, so cannot be resumed";
			else if (!context.getJobStore().updateJobStatus(jobId, QJobStat.Init))
				msg = "failed to resume job " + jobId + " in jobstore";
			if (msg != null) {
				Replyer.replyAbnormal(request, Status.Fail, msg);
				return;
			}
			try {
				context.getQuartzScheduler().resumeJob(sjob);
			} catch (SchedulerException e) {
				// should not happen, since we use RAMJobStore for quartz.
				e.printStackTrace();
			}
			event = TaskEvent.getResumeJobEvent(DomainType.DT, jobId);
			msg = "quartz job " + jobId + " resumed successfully";
		}
		L.info(ZslsConstants.FAKE_DOMAIN_DT, msg);
		context.getTaskDispatcher().getEventHandler().handle(event);	// put event
		Replyer.replyRequest(request);
	}

	/* Handler for asynchronous dispatcher, register requests and write into jobstore */
	@Override
	public void handle(UserRequestEvent event) {
		QRequest request = event.getRequest();
		UserRequest userRequest = request.getUserRequest();
		if (userRequest.getQueryType() == QueryType.ScheduleRequest) {
			IScheduleUnit iunit = userRequest.getScheduleUnit();
			RTJobFlow unit = null;
			if (UnitValidator.checkUnitValid(iunit)) {
				unit = TaskUtil.parseJobUnit(iunit);
				if (unit != null)
					request.setSchedUnit(unit);
			}
			if (unit == null) {
				Replyer.replyAbnormal(request, Status.Invalid, "invalid unit structure");
				return;
			}
			processScheduleRequest(request);
		}
	}
	
	private void processQuartzJob(QRequest request) {
		UserRequest userRequest = request.getUserRequest();
		QuartzTrigger trigger = userRequest.getTrigger();
		IJobFlow jobFlow = userRequest.getJobFlow();
		try {
			ServerQuartzJob serverJob = new ServerQuartzJob(trigger, jobFlow);
			List<String> rpTasks = null;
			synchronized(this) {
				if ((rpTasks = ParamValidator.hasRepeatTasks(jobFlow)) == null)
					QuartzTaskManager.getInstance().putJob(jobFlow.id, serverJob);
			}
			if (rpTasks != null) {
				StringBuilder sb = new StringBuilder("repeat tasks: ");
				for(String t : rpTasks)
					sb.append(" [" + t + "] ");
				Replyer.replyAbnormal(request, Status.Fail, sb.toString());
				return;
			}
			context.getJobStore().storeQuartzJob(serverJob, trigger, userRequest.getQuery());
			context.getQuartzScheduler().scheduleJob(serverJob);
		} catch (Exception e) {
			Replyer.replyAbnormal(request, Status.Fail, "exception when scheduling quartz job " + 
					jobFlow.id + ", " + e);
			return;
		}
		Replyer.replyRequest(request);
	}
	
	
	@SuppressWarnings("unchecked")
	private void processRedoRequest(QRequest request) {
//		DomainType dtype = context.getDomainManager().getDomainType(request.getUserRequest().getDomain());
		TaskEvent event = TaskEvent.getRedoTaskEvent(request);
		context.getTaskDispatcher().getEventHandler().handle(event);
	}
	
	@SuppressWarnings("unchecked")
	private void processMarkRequest(QRequest request) {
		UserRequest userRequest = request.getUserRequest();
		JobType dtype = userRequest.getJobType();
		String taskId = userRequest.getQuery();
		String domain = userRequest.getDomain();
		String msg = null;
		if (dtype == JobType.RT && taskId != null) {
			Task task = LocalJobManager.getRTJobManager(domain).getTask(taskId);
			TaskStat stat = task != null ? task.getStatus() : null;
			if (stat == TaskStat.Fail) {
				boolean ret = context.getJobStore().updateTask(domain, new TaskId(taskId), TaskStat.Finished, null);
				if (ret) {
					TaskEvent event = TaskEvent.getTaskResponseEvent(domain, DomainType.RT, 
							TaskResult.fakeCompleteTaskResult(taskId), new ArrayList<TTaskId>());
					event.setRequest(request);
					context.getTaskDispatcher().getEventHandler().handle(event);
					Replyer.replyRequest(request);
					return;
				} else {
					msg = "failed to mark task " + taskId + " in job store";
				}
			} else {
				msg = "invalid id or status for task " + taskId + ", status: " + stat;
			}
		} else {
			msg = "invalid job type or task id";
		}
		Replyer.replyAbnormal(request, Status.Fail, msg);
	}
	
	
	@SuppressWarnings("unchecked")
	private void processScheduleRequest(QRequest request) {
		UserRequest userRequest = request.getUserRequest();
		String domain = userRequest.getDomain();	
		JobStore jobStore = context.getJobStore();
		RTJobFlow unit = request.getSchedUnit();
		long id = jobStore.instoreJob(unit, userRequest.getScheduleUnit());
		if (id <= 0) {
			Replyer.replyAbnormal(request, Status.Fail, "store failed in server");
			return;
		}
		// dispatch event
		TaskEvent e = TaskEvent.getRTNewJobEvent(domain, unit);
		context.getTaskDispatcher().getEventHandler().handle(e);

		request.resultId = RTJobId.toString(id);
		Replyer.replyRequest(request);
	}
	
	private void disableNode(QRequest request) {
		UserRequest userRequest = request.getUserRequest();
		String domain = userRequest.getDomain();
		NodeId node = userRequest.getWorkerId();
		WorkNode wnode = context.getNodeManager().getNode(domain, node);
		if (wnode == null || !wnode.isRegistered()) {
			Replyer.replyAbnormal(request, Status.Invalid, "WorkNode is unfound or unregistered");
			return ;
		}
		wnode.setDisabled(true);
		Replyer.replyRequest(request);
	}
	
	private void processCMDQuery(QRequest request) {
		UserRequest userRequest = request.getUserRequest();
		String domain = userRequest.getDomain();
		String cmd = userRequest.getQuery();
		boolean err = true;
		String msg = null;
		DomainManager manager = context.getDomainManager();
		WorkerManager nmanager = context.getNodeManager();
		DomainStatus curStat = manager.getDomainStatus(domain);
		do {
			if (curStat == DomainStatus.Prepared) {
				msg = "domain preparing, status cannot be altered";
			}
			else if (UserRequest.PARAM_VAL_PAUSE.equals(cmd)) {
				if (curStat == DomainStatus.Running) {
					err = false;
					manager.changeDomainStatus(domain, DomainStatus.Pause);
				}
			}
			else if (UserRequest.PARAM_VAL_START.equals(cmd)) {
				if (curStat != DomainStatus.Running) {
					err = false;
					manager.changeDomainStatus(domain, DomainStatus.Running);
				}
			}
			else if (UserRequest.PARAM_VAL_STOP.equals(cmd)) {
				if (curStat == DomainStatus.Running) {
					err = false;
					manager.changeDomainStatus(domain, DomainStatus.Stop);
				}
			}
			else if (UserRequest.PARAM_VAL_ADDDM.equals(cmd)) {
				DomainStatus info = manager.getDomainStatus(domain);
				if (info == null && context.getNodeMetaStore().addDomain(domain, DomainType.DT)) {
					manager.addDTDomain(domain);
					manager.changeDomainStatus(domain, DomainStatus.Running);
					err = false;
				}
				else
					msg = domain + " is already existed";
			}
			else if (UserRequest.PARAM_VAL_DELDM.equals(cmd)) {
				DomainStatus stat = manager.getDomainStatus(domain);
				JobType jobType = userRequest.getJobType();
				if (stat == null || ZslsConstants.DEFAULT_DOMAIN.equals(domain))
					msg = "invalid or default domain, which cannot be deleted";
				else if (!nmanager.isDomainEmpty(domain)) 
					msg = domain + " is still running worknodes";
				else if (jobType == JobType.DT) {
					String targetJob = QuartzTaskManager.getInstance().getJobWithTaskInDomain(domain);
					if (targetJob != null)
						msg = "quartzjob " + targetJob + " has task in domain " + 
								domain + ", cancel the job first";
				}
				else {
					synchronized (nmanager.getAddNodeLock()) {
						if (nmanager.isDomainEmpty(domain)) {
							stat = context.getDomainManager()
									.changeDomainStatus(domain, DomainStatus.Abandon);
						}
						else 
							msg = "running nodes starting";
					}
					
					if (msg == null) {
						if (stat == null)
							msg = "invalid domain status for deletion";
						else if (!context.getJobStore().cleanDomain(domain))
							msg = "drop tables failed";
						else
							context.getTaskProcessor().delRTDomain(domain);
					}
				}
				if (msg == null) {
					if (!context.getNodeMetaStore().delDomain(domain)) 
						msg = "nodeMeta delete domain failed";
					else {
						manager.removeDomain(domain);
						context.getNodeManager().delDomain(domain);
						err = false;
					}
					
				}
			}
		} while (false);
		if (err) 
			Replyer.replyAbnormal(request, Status.Invalid, msg);
		else 
			Replyer.replyRequest(request);
	}
	
	private void processStatQuery(QRequest request) {
		UserRequest userRequest = request.getUserRequest();
		Result result = null;
		try {
			switch(userRequest.getSubType()) {
			case Running:
				if (userRequest.getJobType() == JobType.DT) {
					result = queryHelper.queryRunningTaskDT();
					break;
				}
				if (userRequest.getDomain() == null)
					result = queryHelper.queryRunningTask();
				else 
					result = queryHelper.queryRunningUnits(userRequest.getDomain());
				break;
			case Task:
				if (userRequest.jobType == JobType.DT)
					result = queryHelper.queryUnits();
				else
					result = queryHelper.queryUnitByDate(userRequest.getDomain(), userRequest.getTime(), userRequest.getStart(), userRequest.getEnd());
				break;
			case Unit:
				if (userRequest.jobType == JobType.DT)
					result = queryHelper.queryTaskByUnit(userRequest.getUnitId());
				else
					result = queryHelper.queryTaskByUnit(userRequest.getUnitId(), userRequest.getDomain());
				break;
			case Domain:
				if (userRequest.getJobType() == JobType.DT) 
					result = queryHelper.queryDomainDT();
				else
					result = queryHelper.queryDomain();
				break;
			case Swift:
				result = queryHelper.queryUnitBySwift(userRequest.getQuery(), userRequest.getDomain());
				if (result != null && result.data() != null)
					request.setResultId((String)result.data());
				break;
			default:
				result = null;
				break;
			}
		} catch (Exception e) {
			Replyer.replyAbnormal(request, Status.Fail, "exception when process stat requests, " + e);
			return;
		}
		request.setResultInfo(JsonSerilizer.serilize(result.data()));
		Replyer.replyRequest(request);
	}
}
