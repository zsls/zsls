package org.pbccrc.zsls.tasks;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.JobTracker.DomainStatus;
import org.pbccrc.zsls.api.thrift.TaskHandleProtocol;
import org.pbccrc.zsls.api.thrift.records.TTaskId;
import org.pbccrc.zsls.api.thrift.records.TTaskInfo;
import org.pbccrc.zsls.api.thrift.records.TaskHandleRequest;
import org.pbccrc.zsls.api.thrift.records.TaskType;
import org.pbccrc.zsls.api.thrift.utils.RecordUtil;
import org.pbccrc.zsls.collection.CopyOnWriteHashMap;
import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.domain.DomainManager;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.eventdispatch.EventHandler;
import org.pbccrc.zsls.exception.store.JdbcException;
import org.pbccrc.zsls.front.request.QRequest;
import org.pbccrc.zsls.front.request.QRequest.Status;
import org.pbccrc.zsls.front.request.utils.Replyer;
import org.pbccrc.zsls.ha.HAStatus;
import org.pbccrc.zsls.jobengine.JobEngine;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.JobManager;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.nodes.WorkNode;
import org.pbccrc.zsls.rpc.ZuesRPC;
import org.pbccrc.zsls.sched.NodePicker;
import org.pbccrc.zsls.sched.NodePickerFactory;
import org.pbccrc.zsls.sched.rt.RTJobEngine;
import org.pbccrc.zsls.service.AbstractService;
import org.pbccrc.zsls.tasks.MissedTaskCollector.TaskAssignInfo;
import org.pbccrc.zsls.tasks.dt.QuartzTaskInfo;
import org.pbccrc.zsls.tasks.dt.QuartzTaskManager;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobFlow.RJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.tasks.rt.RTTask;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.TaskUtil;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;
import org.pbccrc.zsls.utils.timeout.Expirable;
import org.pbccrc.zsls.utils.timeout.TimeoutHandler;
import org.quartz.SchedulerException;

public class TaskProcessor extends AbstractService implements EventHandler<TaskEvent> {
	
	private static DomainLogger L = DomainLogger.getLogger(TaskProcessor.class.getSimpleName());
	
	// key: domain
	private Map<String, RTJobEngine> rtJobEngines;
	private JobEngine dtJobEngine;
	
	private HashMap<NodeId, TaskHandleProtocol.Iface> assignClients;
	
	private AppContext context;
	private NodePicker nodePicker;
	private UnitMarker unitMarker;
	
	private MissedTaskCollector collector;
	
	private int maxCache;
	private int threshold;
	
	private Object loadLock = new Object();
	
	public TaskProcessor(AppContext context) {
		super(TaskProcessor.class.getSimpleName());
		this.context = context;
	}
	
	protected void serviceInit(Configuration config) throws Exception {
		rtJobEngines = new CopyOnWriteHashMap<String, RTJobEngine>();
		maxCache = config.getInt(ZslsConstants.TASK_CACHE, ZslsConstants.DEFAULT_TASK_CACHE);
		assert(maxCache > 0);
		threshold = (int)(maxCache * 0.8);
		
		JobManager manager = LocalJobManager.getDTJobManager();
		dtJobEngine = new JobEngine(manager);
		// assign clients
		assignClients = new HashMap<NodeId, TaskHandleProtocol.Iface>();
		nodePicker = NodePickerFactory.getNodePicker();
		context.getTimeoutManager().registerHandler(ZslsConstants.TIMEOUT_TASK, new TaskTimeoutHandler());
		// unit marker
		unitMarker = new UnitMarker(context);
		unitMarker.serviceInit(config);
		// missed task collector
		collector = new MissedTaskCollector(context);
		
		super.serviceInit(config);
	}
	
	public synchronized void addRTDomain(String domain) {
		if (LocalJobManager.getRTJobManager(domain) == null) {
			JobManager manager = LocalJobManager.addRTJobManager(domain);
			RTJobEngine engine = new RTJobEngine(domain, manager, maxCache, threshold);
			rtJobEngines.put(domain, engine);
		}
	}
	
	public synchronized void delRTDomain(String domain) {
		if (LocalJobManager.getRTJobManager(domain) != null) {
			LocalJobManager.delRTJobManager(domain);
			rtJobEngines.remove(domain);
		}
	}
	
	protected void serviceStart() throws Exception {
		unitMarker.serviceStart();
		super.serviceStart();
	}
	
	public TaskHandleProtocol.Iface getClient(NodeId id) throws Exception {
		TaskHandleProtocol.Iface client = assignClients.get(id);
		if (client == null) {
			client = ZuesRPC.getRpcClient(TaskHandleProtocol.Iface.class, 
					new InetSocketAddress(id.ip, id.port));
			assignClients.put(id, client);
		}
		return client;
	}
	
	public class TaskTimeoutHandler implements TimeoutHandler {
		@Override
		public void handleTimeout(Expirable item) {
			Task task = (Task)item;
			String domain = task.getDomain();
			TaskId taskId = new TaskId(task.getTaskId());
			WorkNode node = context.getNodeManager().getNodeWithAssignedTask(domain, taskId);
			L.error(domain, "task " + task.getTaskId() + " timeout on node " + node);
			DomainType type = context.getDomainManager().getDomainType(domain);
			JobManager manager = type == DomainType.DT ? LocalJobManager.getDTJobManager() :
				LocalJobManager.getRTJobManager(domain);
			if (manager != null) {
				Task regTask = manager.getTask(taskId.id);
				if (regTask != null)
					regTask.markStatus(TaskStat.Timeout);
			}
		}
	}
	
	public JobEngine getJobEngine(String domain, DomainType dtype) {
		if (dtype == DomainType.DT)
			return dtJobEngine;
		else
			return rtJobEngines.get(domain);
	}
	
	private boolean checkRunningTasks(String domain, DomainType dtype, JobEngine engine, 
			WorkNode node, List<TTaskId> tasks) {
		List<TaskAssignInfo> missedTasks = collector.checkRunningTask(domain, node, tasks);
		if (missedTasks.size() <= 0)
			return false;
		if (missedTasks.size() > 0) {
			L.warn(domain, "collect " + missedTasks.size() + " tasks");
			JobManager manager = LocalJobManager.getJobManager(domain, dtype);
			for (TaskAssignInfo info : missedTasks) {
				L.warn(domain, "collect untracked task " + info.taskId + " back from node " + info.node.getNodeId());
				node.removeTask(new TaskId(info.taskId));
				Task task = manager.getTask(info.taskId);
				if (task != null) {
					context.getTimeoutManager().cancelTimeout(task);
					task.markStatus(TaskStat.Init);
					if (engine != null)
						engine.addToExecutableQueue(task);
				}
			}
		}
		return true;
	}

	@Override
	public void handle(TaskEvent event) {
		String domain = event.getDomain();
		DomainType dtype = event.getDomainType();
		DomainManager dmanager = context.getDomainManager();
		switch (event.getType()) {
		case UPDATE_RUNNING:
			WorkNode node = context.getNodeManager().getNode(domain, event.getNode().getNodeId());
			JobEngine engine = dtype == DomainType.RT ? rtJobEngines.get(domain) : dtJobEngine;
			boolean hasMissedTask = checkRunningTasks(domain, dtype, engine, node, event.getTasks());
			if (hasMissedTask && dmanager.getDomainStatus(domain) == DomainStatus.Running) {
				tryAssignTask(engine);
			}
			break;
		case COMPLETE:
		case FAIL:
			TaskResult tr = event.getResult();
			NodeId id = tr.getNodeId();
			TaskId task = tr.getTaskId();
			node = context.getNodeManager().getNode(domain, id);
			if (!id.isFake() && node == null) {
				L.error(domain, "completed task " + task + " from unregistered node: " + id);
				return;
			}
			
			if (L.logger().isDebugEnabled())
				L.debug(domain, "task " + task + " completed from node " + id);
			
			engine = dtype == DomainType.RT ? rtJobEngines.get(domain) : dtJobEngine;
			if (!id.isFake() && node != null) {
				checkRunningTasks(domain, dtype, engine, node, event.getTasks());
				if (!node.removeTask(task))
					L.error(domain, "completed task " + task + " not registered in node: " + id);
			}
			
			if (engine == null) break;
			
			JobFlow u = engine.complete(task, tr);
			if (u != null) {
				L.info(domain, "Job " + u.getJobId().toString() + " finished");
				// mark unit asynchronously. it's not important, since it's just 
				// used for initial loading
				if (dtype == DomainType.RT) {
					unitMarker.rtUnitFinish(domain, u.getJobId());
					maybeTryLoadRTJobs(domain);		
				}
				else {
					ServerQuartzJob job = QuartzTaskManager.getInstance().getJob(u.getJobId().toString());
					if (job != null && job.changeStatus(QJobStat.Finish) != null) {
						job.updateRuntimeParams(task.id, tr.getRuntimeParam());	
						unitMarker.dtUnitFinish(domain, u.getJobId());
					}
				}
			}
			
			// only can assign tasks after the loading of runtime info from all
			// nodes completed.
			if (dmanager.getDomainStatus(domain) == DomainStatus.Running)
				tryAssignTask(engine);
			break;
			
		case REDO_TASK:	// only RT
			QRequest request = event.getRequest();
			domain = request.getUserRequest().getDomain();
			task = new TaskId(request.getUserRequest().getQuery());
			engine = rtJobEngines.get(domain);
			if (engine == null) {
				Replyer.replyAbnormal(request, Status.Invalid, "no engine for domain " + domain);
				break;
			}
			Task regTask = LocalJobManager.getRTJobManager(domain).getTask(task.id);
			RTTask userTask = regTask != null ? (RTTask)regTask : null;
			boolean success = false;
			if (userTask == null) {
				RTJobId uid = RTJobId.fromString(task.id);
				if (uid != null) {
					RTJobFlow unit = context.getJobStore().fetchJob(domain, uid);
					regTask = unit != null ? unit.getTask(task.id) : null;
					userTask = regTask != null ? (RTTask)regTask : null;
					// unit必须为结束. 否则该unit就是还没有开始调度的单元。
					if (unit != null && unit.isFinished() && userTask != null) {
						userTask.markReSubmit();
						unitMarker.unitResubmit(domain, uid);
						unit.markJobFinish(false);
						context.getJobStore().updateTask(domain, task, TaskStat.ReSubmit, null);
						engine.feed(unit);
						success = true;
					}
				}
			}
			else if (userTask.canRedo()) {
				userTask.markReSubmit();
				engine.addToExecutableQueue(userTask);
				context.getJobStore().updateTask(domain, task, TaskStat.ReSubmit, null);
				success = true;
			}
			if (success) {
				Replyer.replyRequest(request);
				if (engine != null && dmanager.getDomainStatus(domain) == DomainStatus.Running)
					tryAssignTask(engine);
			}
			else {
				Replyer.replyAbnormal(request, Status.Invalid, 
						"invalid task or task status " + task);
			}
			break;
			
		case RT_NEW_JOB:
			RTJobFlow unit = event.getUnit();
			RTJobEngine rtengine = rtJobEngines.get(domain);
			if (rtengine != null && rtengine.isSync()) {
				if (unit.getTaskNum() < rtengine.getLoadCapacity())
					rtengine.feed(unit);
				else {
					rtengine.setSync(false);
					L.info(domain, "engine go into async");
				}
			}
		case RT_NEW_NODE:
		case RT_TRIGGER:
			rtengine = rtJobEngines.get(domain);
			maybeTryLoadRTJobs(domain);
			if (rtengine != null && dmanager.getDomainStatus(domain) == DomainStatus.Running)
				tryAssignTask(rtengine);
			break;
			
		case DT_NEW_JOB:
			QuartzTaskInfo qtask = event.getQuartzTaskInfo();
			qtask.getFuture().done();
			JobFlow job = qtask.getJob();
			ServerQuartzJob sjob = QuartzTaskManager.getInstance().getJob(job.getJobId().toString());
			if (sjob != null && sjob.changeStatus(QJobStat.Run) != null) {
				if (dtJobEngine.feed(job) < 0)
					L.error(ZslsConstants.FAKE_DOMAIN_DT, "DT job " + job.getJobId() + " not finished yet");
				else {
					sjob.setLastExecuteTime(job.getGenerateTime());
					tryAssignTask(dtJobEngine);
				}
			}
			break;
			
		case DT_TRIGGER:
			if (dmanager.isAllDTDomainsReady())
				tryAssignTask(dtJobEngine);
			break;
			
		case KILL_JOB:
			if (event.getDomainType() == DomainType.DT) {	// only DT for now.
				String jobId = event.getJobId();
				sjob = QuartzTaskManager.getInstance().getJob(jobId);
				if (sjob == null) {
					L.error(ZslsConstants.FAKE_DOMAIN_DT, "quartz job " + jobId + " not found, abort killing.");
					return;
				}
				QJobStat oldStat = sjob.changeStatus(QJobStat.Cancel);
				L.info(ZslsConstants.FAKE_DOMAIN_DT, "kill DT job " + jobId + ", oldStat: " + oldStat);
				// send killing requests
				JobFlow tmpJob = sjob.generateJobInstance();
				Map<WorkNode, List<String>> nodes = context.getNodeManager().
						getNodesWithRunningJob(tmpJob);
				tryKillTasks(nodes);
				// unregister job
				for (WorkNode n : nodes.keySet()) {
					List<String> list = nodes.get(n);
					if (list == null) continue;
					for (String t : list)
						n.removeTask(new TaskId(t));
				}
				for (Task t : tmpJob.getTasks())
					dtJobEngine.removeFromExecutableQueue(t.getTaskId());
				LocalJobManager.getDTJobManager().unregister(jobId);
			}
			break;
			
		case RESUME_JOB:	// only DT
			String jobId = event.getJobId();
			sjob = QuartzTaskManager.getInstance().getJob(jobId);
			if (sjob == null) {
				L.error(ZslsConstants.FAKE_DOMAIN_DT, "quartz job " + jobId + " not found, abort resume.");
				return;
			}
			if (sjob.changeStatus(QJobStat.Init) == null)
				L.error(ZslsConstants.FAKE_DOMAIN_DT, "failed to change job status to resume, curstat: " 
						+ sjob.getJobStat());
			break;
			
		default:
			break;
		}
	}
	
	public void tryLoadDTJobs() throws SchedulerException {
		List<ServerQuartzJob> jobs = context.getJobStore().fetchQuartzJobs();
		StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
		L.info(ZslsConstants.FAKE_DOMAIN_DT, "All DT jobs... ");
		for (ServerQuartzJob job : jobs) {
			b.append("\t[").append(job).append("]\n");
		}
		for (ServerQuartzJob job : jobs) {
			QuartzTaskManager.getInstance().putJob(job.getJobId(), job);
			if (job.getJobStat() == QJobStat.Run) {
				L.info(ZslsConstants.FAKE_DOMAIN_DT, "now feed unfinished DT job " + job.getJobId());
				Date date = job.getLastExecuteTime();
				JobFlow jobFlow = job.generateJobInstance();
				jobFlow.setGenerateTime(date);
				context.getJobStore().updateTaskStatsForJob(jobFlow, date);
				if (dtJobEngine.feed(jobFlow) == 0 && jobFlow.isFinished()) {
					L.warn(ZslsConstants.FAKE_DOMAIN_DT, "feed finished DT job " + jobFlow.getJobId());
					context.getJobStore().updateJobStatus(job.getJobId(), QJobStat.Finish);
					job.changeStatus(QJobStat.Finish);
				}
			}
		}
	}
	
	public void maybeTryLoadRTJobs(String domain) {
		try {
		RTJobEngine engine = (RTJobEngine)rtJobEngines.get(domain);
		if (engine != null && !engine.isSync() && engine.canReload()) {
			// just inited
			RTJobId latestId = context.getJobStore().getFirstUnfinishedUnit(domain);
			if (latestId == null) {
				engine.setSync(true);
				L.info(domain, "no jobs to load, engine go into sync");
				return;
			}
			L.info(domain, "try load from unitid " + latestId.getId());
			List<RTJobFlow> jobs = new LinkedList<RTJobFlow>();
			boolean loadedAll = context.getJobStore().fetchJobs(domain, latestId,
					jobs, engine.getLoadCapacity());
			
			StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
			b.append("load [");
			for (RTJobFlow unit : jobs) {
				b.append(unit.getJobId().toString());
				if (engine.feed(unit) == 0 || unit.isFinished()) {
					context.getJobStore().updateJob(domain, unit.getJobId(), RJobStat.Finished);
					b.append("(Finished)");
				}
				b.append(",");
			}
			b.append("]");
			L.info(domain, b.toString());
			if (loadedAll) {
				engine.setSync(true);
				L.info(domain, "engine go into sync");
			}
		}	
		} catch (JdbcException e) {
			L.error(domain, "exception when tryLoadRTJobStore -> " + e);
			e.printStackTrace();
		}
	}
	
	private List<WorkNode> getAssignableNodes(Task task) {
		String domain = task.getDomain();
		List<WorkNode> list = null;
		if (domain == ZslsConstants.DEFAULT_DOMAIN) {
			list = new ArrayList<WorkNode>(1);
			NodeId nodeId = NodeId.getNodeId(domain, task.getTargetNode());
			WorkNode node = context.getNodeManager().getNode(domain, nodeId);
			if (node != null)
				list.add(node);
		} else {
			list = nodePicker.getAssignableNodes(context.getNodeManager(), domain);
		}
		return list;
	}
	
	private void tryAssignTask(JobEngine engine) {
		if (context.getHAStatus() != HAStatus.MASTER)
			return;
		Task task = null;
		while ((task = engine.next()) != null) {
			if (!task.canBeAssigned())
				continue;
			String domain = task.getDomain();
			List<WorkNode> nodes = getAssignableNodes(task);
			Iterator<WorkNode> it = nodes.iterator();
			boolean sent = false;
			while (!sent && it.hasNext()) {
				WorkNode node = it.next();
				if (!node.isRegistered())
					continue;
				TaskHandleRequest request = new TaskHandleRequest();
				request.setTaskType(TaskType.NORMAL);
				TTaskInfo info = RecordUtil.trans(TaskUtil.getTaskInfo(task));
				request.setTaskInfo(info);
				if (task instanceof RTTask)
					request.setRetryTask(((RTTask)task).getAssignCount() > 0);
				try {
					if (doAssign(request, node)) {
						L.info(domain, "assign task " + task.getTaskId() + " to node " + node.getNodeId());
						task.markAssigned();
						node.addTask(new TaskId(task.getTaskId()));
						context.getTimeoutManager().add(task.resetExpireTime(), ZslsConstants.TIMEOUT_TASK);
						sent = true;
					}
				} catch (Exception e) {
					L.error(domain, "Exception when assigning task " + task + " to node " + node + ", " + e);
				}
			}
			if (!sent) {
				engine.addToExecutableQueue(task);
				if (nodes.isEmpty() && engine == dtJobEngine)
					L.info(domain, "No worker available for task " + task.getTaskId());
				break;
			}
		}
	}
	
	private void tryKillTasks(Map<WorkNode, List<String>> nodes) {
		for (WorkNode n : nodes.keySet()) {
			List<String> tasks = nodes.get(n);
			if (tasks == null) continue;
			for (String t : tasks) {
				TaskHandleRequest request = new TaskHandleRequest();
				TTaskInfo info = new TTaskInfo();
				TTaskId id = new TTaskId();
				id.taskid = t;
				info.setTaskid(id);
				request.setTaskInfo(info);
				try {
					L.info(n.getDomain(), "try kill task " + t + " in node " + n.getNodeId());
					doKill(request, n);
				} catch (Exception e) {
					L.error(n.getDomain(), "Exception when try kill task " + t + " on node " + n.getNodeId());
					e.printStackTrace();
					//TODO: maybe should re-send
				}
			}
		}
	}
	
	
	private boolean doAssign(TaskHandleRequest request, WorkNode node) throws Exception {
		return doSendTaskHandleRequest(request, node, true);
	}
	private boolean doKill(TaskHandleRequest request, WorkNode node) throws Exception {
		return doSendTaskHandleRequest(request, node, false);
	}
	
	private boolean doSendTaskHandleRequest(TaskHandleRequest request, WorkNode node, 
			boolean assign) throws Exception {
		TaskHandleProtocol.Iface client = null;
		try {
			client = getClient(node.getNodeId());
		} catch (Exception e) {
			// exception when acquiring client, throw directly
			throw e;	
		}
		
		try {
			if (assign)
				client.assignTask(request);
			else
				client.killTask(request);
		} catch (Exception e) {
			// a second chance is given when exception occurs during sending request,
			// in case the former connection disconnected by accident, since thrift uses
			// BIO for sync-client. TODO: replace clients with async ones
			ZuesRPC.closeClient(client);
			NodeId id = node.getNodeId();
			this.assignClients.remove(id);
			try {
				client = ZuesRPC.getRpcClient(TaskHandleProtocol.Iface.class, 
					new InetSocketAddress(id.ip, id.port));
				if (client != null) {
					this.assignClients.put(id, client);
					if (assign)
						client.assignTask(request);	
					else
						client.killTask(request);
				}
			} catch (Exception ee) {
				this.assignClients.remove(id);
				throw ee;
			} 
		}
		return true;
	}
	
	public Object getLoadLock() {
		return loadLock;
	}
	
}
