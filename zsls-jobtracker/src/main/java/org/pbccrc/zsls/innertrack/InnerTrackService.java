package org.pbccrc.zsls.innertrack;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.pbccrc.zsls.JobTracker.DomainStatus;
import org.pbccrc.zsls.api.thrift.InnerTrackerProtocol;
import org.pbccrc.zsls.api.thrift.records.HeartBeatRequest;
import org.pbccrc.zsls.api.thrift.records.HeartBeatResponse;
import org.pbccrc.zsls.api.thrift.records.NodeAction;
import org.pbccrc.zsls.api.thrift.records.RegisterRequest;
import org.pbccrc.zsls.api.thrift.records.RegisterResponse;
import org.pbccrc.zsls.api.thrift.records.ReportTaskRequest;
import org.pbccrc.zsls.api.thrift.records.ReportTaskResponse;
import org.pbccrc.zsls.api.thrift.records.TTaskId;
import org.pbccrc.zsls.api.thrift.records.TTaskResult;
import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.api.thrift.utils.RecordUtil;
import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.RegistryConfig;
import org.pbccrc.zsls.config.ServerConfig;
import org.pbccrc.zsls.config.WhiteListConfig;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.dataload.DataLoader;
import org.pbccrc.zsls.domain.DomainManager;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.ClusterInfo;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.exception.ZslsRuntimeException;
import org.pbccrc.zsls.ha.HAStatus;
import org.pbccrc.zsls.jobengine.JobEngine;
import org.pbccrc.zsls.jobengine.JobManager;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobengine.Task.ExecuteResult;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.nodes.NodeIdInfo;
import org.pbccrc.zsls.nodes.WNodeLiveMonitor;
import org.pbccrc.zsls.nodes.WorkNode;
import org.pbccrc.zsls.nodes.WorkerManager;
import org.pbccrc.zsls.nodes.store.NodeMeta;
import org.pbccrc.zsls.nodes.store.NodeMetaStore;
import org.pbccrc.zsls.registry.RegNode;
import org.pbccrc.zsls.rpc.ZuesRPC;
import org.pbccrc.zsls.rpc.server.Server;
import org.pbccrc.zsls.service.CompositeService;
import org.pbccrc.zsls.tasks.LocalJobManager;
import org.pbccrc.zsls.tasks.TaskEvent;
import org.pbccrc.zsls.tasks.dt.QuartzTaskManager;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;
import org.quartz.SchedulerException;

public class InnerTrackService extends CompositeService implements InnerTrackerProtocol.Iface {
	
	private static DomainLogger L = DomainLogger.getLogger(InnerTrackService.class.getSimpleName());
	
	private AppContext context;
	
	private WNodeLiveMonitor nodeLiveMonitor;
	private int heartBeatInterval;
	
	private Server server;
	private ServerConfig config;
	private RegistryConfig registryConfig;
	
	private DataLoader dataLoader;
	
	private WhiteListData whiteListData;
	private WhiteListConfig whiteConfig;
	
	private NodeMetaStore nodeMetaStore;

	private InnerTrackService(String name) {
		super(name);
	}
	public InnerTrackService(AppContext context, WNodeLiveMonitor nodeLiveMonitor) {
		super(InnerTrackService.class.getSimpleName());
		this.context = context;
		this.nodeLiveMonitor = nodeLiveMonitor;
		this.nodeMetaStore = context.getNodeMetaStore();
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		// server and dataLoader are not services.
		config = ServerConfig.readConfig(conf, ServerConfig.NAME_TRACK);
		registryConfig = RegistryConfig.readConfig(conf);
		InetSocketAddress addr = new InetSocketAddress(config.getPort());
		server = ZuesRPC.getRpcServer(InnerTrackerProtocol.Iface.class, this, addr, 
				config.getIoThreads(), config.getWorkerThreads()); 
		heartBeatInterval = conf.getInt(ZslsConstants.HEART_BEAT_INTERVAL, ZslsConstants.DEFAULT_HEART_BEAT_INTERVAL);
		if (heartBeatInterval < 0)
			throw new ZslsRuntimeException("invalid heartbeat interval");
		
		whiteConfig = WhiteListConfig.read(conf);
		whiteListData = new WhiteListData(whiteConfig.getFilePath());
		dataLoader = new DataLoader();
		dataLoader.setInterval(whiteConfig.getUpdateInterval());
		dataLoader.addSource(whiteListData);
		
		super.serviceInit(conf);
	}
	
	protected void serviceStart() throws Exception {
		super.serviceStart();
		dataLoader.loadOnceAndStart();
		server.start();
		L.info(DomainLogger.SYS, "inner track server started on port: " + config.getPort());
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public RegisterResponse regiserNode(RegisterRequest request) throws TException {
		NodeId id = RecordUtil.trans(request.getNodeid());
		String domain = request.getDomain();
		L.info(domain, "receive register request from " + id);
		
		RegisterResponse response = new RegisterResponse();
		
		// check ha status
		if (context.getHAStatus() != HAStatus.MASTER) {
			response.setNodeAction(NodeAction.NOT_MASTER);
			if (context.getHAStatus() == HAStatus.STANDBY) {
				RegNode node = context.getRegistry().getMaster();
				String masterAddr = null;
				if (node != null) {
					masterAddr = node.getIp() + ":" + node.getPort();
					ClusterInfo info = new ClusterInfo(masterAddr);
					response.setCluster(RecordUtil.trans(info));	
				}
				L.info(domain, NodeAction.NOT_MASTER + 
							" register response with current master " + masterAddr);
			}
			return response;
		}
		// check white_list and task config
		if ((whiteConfig.isEnabled() && !whiteListData.contains(id.ip))
				|| request.getMaxnum() <= 0) {
			return setInvalidResponse(response, domain, 
					"register request validation fail due to max_tasknum/whitelist: [" 
							+ request.getMaxnum() + ", " + id.ip + "]");
		}
		
		// handle new domains
		DomainManager manager = context.getDomainManager();
		DomainType dtype = request.isIsDt() ? DomainType.DT : DomainType.RT;
		if (!manager.containsDomain(domain) && manager.addDomain(domain, dtype)) {
			if (!nodeMetaStore.addDomain(domain, dtype)) {
				manager.removeDomain(domain);	// don't remember to remove domain
				return setInvalidResponse(response, domain, "NodeMetaStore failed to add domain " + domain);
			}
			if (dtype == DomainType.RT) {
				if (!context.getJobStore().initForDomain(domain)) {
					manager.removeDomain(domain);
					nodeMetaStore.delDomain(domain);
					return setInvalidResponse(response, domain, "JobStore failed to init domain " + domain);
				}
				context.getTaskProcessor().addRTDomain(domain);
			}
			manager.changeDomainStatus(domain, DomainStatus.Prepared);
		}
		if (manager.getDomainType(domain) != dtype)
			return setInvalidResponse(response, domain, 
					"domain "  + domain + " had registered to " + manager.getDomainType(domain) + ", rename and retry");
		// another register request just arrived earlier
		DomainStatus status = manager.getDomainStatus(domain);
		if (status == null || status == DomainStatus.Init || status == DomainStatus.Abandon)
			return setInvalidResponse(response, domain, 
					"invalid domain status " + status + ", retry later");
		
		// store node meta 
		NodeMeta meta = new NodeMeta(domain, id, request.getMaxnum());
		if (!nodeMetaStore.storeOrUpdate(meta))
			return setInvalidResponse(response, domain, "server store node meta failed: " + meta);
		
		// register and update info
		WorkerManager nmanager = context.getNodeManager();
		WorkNode oldNode = nmanager.getNode(domain, id);
		if (oldNode != null) {
			L.info(domain, "node already registered: " + id);
			oldNode.updateMeta(request.getMaxnum());
			nodeLiveMonitor.register(new NodeIdInfo(id, domain));
			
			// possibilities:
			// 1. jobtracker just started/restarted
			// 2. tasktracker just restarted in a short time before jobtracker be aware of its death.
			
			// info from new requests are always trusted no matter in which case it is.
			loadAssignedInfoFromRegRequests(request, domain, oldNode);
			oldNode.setRegistered(true);
			
			// just start up, may be need to update domain status.
			if (manager.getDomainStatus(domain) == DomainStatus.Prepared) {
				if (context.getNodeManager().isAllRegistered(domain)) {
					manager.changeDomainStatus(domain, DomainStatus.Running);
					L.info(domain, "register loading completion, domainStatus -> " + DomainStatus.Running);
				}
			}
		}
		else {
			WorkNode node = new WorkNode(domain, id, context, request.getMaxnum());
			L.info(domain, "new node registered: " + node);
			switch (status) {
			case Running:
				synchronized (nmanager.getAddNodeLock()) {
					if (status != DomainStatus.Abandon) {
						context.getNodeManager().addWorkNode(domain, node);
						nodeLiveMonitor.register(new NodeIdInfo(id, domain));
					} else {
						return setInvalidResponse(response, domain, "");
					}
				}
				break;
			case Prepared:
				context.getNodeManager().addWorkNode(domain, node);
				nodeLiveMonitor.register(new NodeIdInfo(id, domain));
				loadAssignedInfoFromRegRequests(request, domain, node);
				if (context.getNodeManager().isAllRegistered(domain)) {
					manager.changeDomainStatus(domain, DomainStatus.Running);
					L.info(domain, "register loading completion");
				}
			default:
				break;
			}
		}
		if (dtype == DomainType.DT && manager.isAllDTDomainsReady()) {
			try {
				context.getQuartzScheduler().scheduleJobs();
			} catch (SchedulerException e) {
				L.fatal(ZslsConstants.FAKE_DOMAIN_DT, " exception when scheduling DT jobs !!! ");
				e.printStackTrace();
				System.exit(-1);
			}
			TaskEvent event = TaskEvent.getTriggerEvent(domain, dtype);
			context.getTaskDispatcher().getEventHandler().handle(event);
		} else if (dtype == DomainType.RT) {
			TaskEvent event = TaskEvent.getNewNodeEvent(domain, dtype);
			context.getTaskDispatcher().getEventHandler().handle(event);
		}
		
		// response
		response.setNodeAction(NodeAction.NORMAL);
		response.setHeartBeatInterval(heartBeatInterval);
		response.setRegistrySessTimeout((int)(registryConfig.getSessionTimeout()));
		return response;
	}
	
	// 更新任务信息
	private void loadAssignedInfoFromRegRequests(RegisterRequest request, String domain, WorkNode node) {
		DomainType dtype = context.getDomainManager().getDomainType(domain);
		JobEngine engine = context.getTaskProcessor().getJobEngine(domain, dtype);
		JobManager manager = LocalJobManager.getJobManager(domain, dtype);
		
		// 如果在jobtracker中节点有任务执行，而一个已经注册过的节点再次注册的时候，很可能是
		// 节点关闭之后很快进行了重启，对jobtracker来讲没有变化，但是之前分配的部分任务可能
		// 已经丢失，这些任务需要重新分配。
		// TODO DT和RT暂时做相同处理
		List<TTaskId> running = request.getRunningTasks();
		Set<TaskId> reportRunning = new HashSet<TaskId>();
		for (TTaskId tmp : running)
			reportRunning.add(new TaskId(tmp.taskid));
		Iterator<TaskId> it = node.getRunningTasks().iterator();
		while (it.hasNext()) {
			TaskId task = it.next();
			if (!reportRunning.contains(task)) {
				Task t = manager.getTask(task.id);
				if (t != null) {
					t.markStatus(TaskStat.Init);
					engine.addToExecutableQueue(t);
					it.remove();
					L.info(domain, "re-schedule task " + task + 
							" that missed in register request from node " + node.getNodeId());
				}
			}
		}
		StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
		b.append("loading assigned info from node ").append(node).append(" [");
		for (TaskId tid : reportRunning) {
			b.append(tid);
			Task t = manager.getTask(tid.id);
			if (t != null) {
				t.markAssigned();
				if (!node.isRunningTask(tid))
					node.addTask(tid);
				b.append("(ok) ");
				context.getTimeoutManager().add(t.resetExpireTime(), ZslsConstants.TIMEOUT_TASK);
			} else {
				b.append("(miss) ");
			}
		}
		b.append("]");
		L.info(domain, b.toString());
	}
	
	private RegisterResponse setInvalidResponse(RegisterResponse response, String domain, String msg) {
		response.setNodeAction(NodeAction.INVALID);
		response.setMessage(msg);
		L.warn(domain, msg);
		return response;
	}
	
	private void setAbnormalResponse(HeartBeatResponse response, NodeAction action, 
			String domain, String msg) {
		response.setNodeAction(NodeAction.INVALID);
		L.warn(domain, msg);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public HeartBeatResponse heartBeat(HeartBeatRequest request) throws TException {
		HeartBeatResponse response = new HeartBeatResponse();
		String domain = request.getDomain();
		// validate node and registration info
		NodeId id = RecordUtil.trans(request.getNodeid());
		WorkNode node = context.getNodeManager().getNode(domain, id);
		if (node == null || !node.isRegistered()) {
			setAbnormalResponse(response, NodeAction.RE_REGISTER, request.getDomain(),
					"heartbeat from unregistered node: " + id + ", require reregister");
			return response;
		}
		// HA status
		if (context.getHAStatus() != HAStatus.MASTER) {
			setAbnormalResponse(response, NodeAction.RE_REGISTER, request.getDomain(),
					"ha status " + context.getHAStatus() + " for heart beat from " 
					+ request.getNodeid() + ", require reregister");
			return response;
		}
		nodeLiveMonitor.receivedPing(new NodeIdInfo(id, request.getDomain()));
		response.setNodeAction(NodeAction.NORMAL);
		if (L.logger().isDebugEnabled()) {
			List<TTaskId> runningTasks = request.getRunningTasks();
			StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
			b.append("receive heart beat from ").append(id).append(" [");
			for (TTaskId t : runningTasks) {
				b.append(t).append(",");
			}
			b.append("]");
			L.debug(request.getDomain(), b.toString());
		}
		// dispatch event
		DomainType dtype = context.getDomainManager().getDomainType(domain);
		TaskEvent event = TaskEvent.getUpdateRunningEvent(domain, dtype, node, request.getRunningTasks());
		context.getTaskDispatcher().getEventHandler().handle(event);
		
		return response;
	}
	
	
	@Override
	public ReportTaskResponse taskComplete(ReportTaskRequest request) throws TException {
		NodeId id = RecordUtil.trans(request.getNodeId());
		String domain = request.getDomain();
		ReportTaskResponse response = new ReportTaskResponse();
		// HA status
		if (context.getHAStatus() != HAStatus.MASTER) {
			response.setNodeAction(NodeAction.RE_REGISTER);
			L.warn(domain, context.getHAStatus() + " ha status for task report, require reregister");
			return response;
		}
		// node status
		WorkNode node = context.getNodeManager().getNode(request.getDomain(), id);
		if (node == null || !node.isRegistered()) {
			response.setNodeAction(NodeAction.RE_REGISTER);
			L.warn(domain, "task report from unregistered node: " + id + ", require reregister");
			return response;
		}
		// do handle request
		nodeLiveMonitor.receivedPing(new NodeIdInfo(id, request.getDomain()));
		if (handleTaskReport(domain, request.getTaskResults(), request.getRunningTasks(), id, "task report received"))
			response.setNodeAction(NodeAction.NORMAL);
		else
			response.setNodeAction(NodeAction.INVALID);
		return response;
	}
	
	@SuppressWarnings("unchecked")
	private boolean handleTaskReport(String domain, List<TTaskResult> results, List<TTaskId> runningTasks,
			NodeId id, String msg) {
		List<TaskResult> list = new ArrayList<TaskResult>();
		StringBuilder builder = ThreadLocalBuffer.getLogBuilder(0);
		builder.append(msg).append(", finished tasks -> ");
		for (TTaskResult r : results) {
			TaskResult re = RecordUtil.trans(r, id);
			list.add(re);
			builder.append("[").append(re.getTaskId()).append(": ").append(re.getAction()).append("]");
		}
		L.info(domain, builder.toString());
		
		try {
			for (TaskResult r : list) {
				switch (r.getAction()) {
				case COMPLETE:
				case FAILED:
					TaskStat stat = r.getAction() == TaskAction.COMPLETE ? 
							TaskStat.Finished : TaskStat.Fail;
					DomainType dtype = context.getDomainManager().getDomainType(domain);
					if (dtype == null) {
						L.error(domain, "unkown domain, drop report");
						return true;
					}
					if (dtype == DomainType.RT) {
						context.getJobStore().updateTask(domain, r.getTaskId(), stat, 
								new ExecuteResult(r.getKeyMessage(), r.getAppendInfo()));
					}
					else {
						String taskId = r.getTaskId().toString();
						JobManager manager = LocalJobManager.getDTJobManager();
						Task task = manager.getTask(taskId);
						String errMsg = null;
						if (task != null) {
							String jobId = task.getJobFlow().getJobId().toString();
							ServerQuartzJob sjob = QuartzTaskManager.getInstance().getJob(jobId);
							if (sjob == null)
								errMsg = "no job registered for task " + taskId + ", probably canceled";
							else
								if (!context.getJobStore().updateTaskResult(jobId, taskId, r.getDate(), r))
									errMsg = "failed to update task result for " + taskId + "(" + r.getDate()+ ")";
						} else {
							errMsg = "DT task " + taskId + " not registered, probably canceled";
						}
						if (errMsg != null)
							L.error(domain, errMsg);
					}
					// dispatch events
					TaskEvent event = TaskEvent.getTaskResponseEvent(domain, dtype, r, runningTasks);
					context.getTaskDispatcher().getEventHandler().handle(event);
					break;
					
				default:
					break;
				}
			}
		} catch (Exception e) {
			L.error(domain, "exception when updating task result " + e);
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

}
