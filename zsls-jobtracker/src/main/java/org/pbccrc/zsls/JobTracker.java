package org.pbccrc.zsls;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.RegistryConfig;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.context.AppContextImpl;
import org.pbccrc.zsls.domain.DomainManager;
import org.pbccrc.zsls.eventdispatch.AsyncDispatcher;
import org.pbccrc.zsls.eventdispatch.Dispatcher;
import org.pbccrc.zsls.exception.ZslsRuntimeException;
import org.pbccrc.zsls.front.http.HttpFrontService;
import org.pbccrc.zsls.ha.HAStatus;
import org.pbccrc.zsls.ha.MasterMonitor;
import org.pbccrc.zsls.innertrack.InnerTrackService;
import org.pbccrc.zsls.jobstore.JobStore;
import org.pbccrc.zsls.jobstore.JobStoreFactory;
import org.pbccrc.zsls.nodes.NodeEventType;
import org.pbccrc.zsls.nodes.NodeProcessor;
import org.pbccrc.zsls.nodes.WNodeLiveMonitor;
import org.pbccrc.zsls.nodes.WorkerManager;
import org.pbccrc.zsls.nodes.store.NodeMetaStore;
import org.pbccrc.zsls.nodes.store.NodeMetaStoreFactory;
import org.pbccrc.zsls.registry.MasterRegistry;
import org.pbccrc.zsls.registry.NotifyEvent;
import org.pbccrc.zsls.registry.RegistryFactory;
import org.pbccrc.zsls.sched.QuartzScheduler;
import org.pbccrc.zsls.service.CompositeService;
import org.pbccrc.zsls.tasks.HubDispatcher;
import org.pbccrc.zsls.tasks.TaskEventType;
import org.pbccrc.zsls.tasks.TaskProcessor;
import org.pbccrc.zsls.tasks.dt.QuartzTaskManager;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.JobStoreHelper;
import org.pbccrc.zsls.utils.timeout.TimeoutManager;

/**
 * JobTracker Main
 * 
 * Communication mechanism of <code>JobTracker</code> and <code>TaskTracker</code>:
 * 1. TaskTracker first register to JobTracker on startup.(or receive ReRegister response during runtime)
 * 2. then JobTracker respond with runtime meta (like heart beat interval, etc) after validation of the TaskTracker.
 * 3. TaskTracker heart beat to JobTracker periodically to maintain of its liveness.
 * 4. JobTracker would assign tasks to TaskTracker.
 * 5. TaskTracker report task completion information after execution of tasks.
 * 
 * Important Features:
 * 1. Thrift is utilized as the RPC framework of the inner system, while Netty is used to provide external HTTP services.
 * 2. HA of JobTracker is supported, yet only one JobTracker would become master and provide service at one moment.
 * 		HA is achieved by interface {@link org.pbccrc.zsls.registry.MasterRegistry}, with ZooKeeper as its only 
 * 		implementation for now.
 * 3. Runtime info of assigned tasks are not store in JobTracker, so when a standby JobTracker just becomes master,
 * 		apart from loading task info from {@link org.pbccrc.zsls.jobstore.JobStore}, it would also reply on 
 * 		TaskTrackers' registration to recover former assigned task info.
 * 4. Currently supported JobStores are MySql, Oracle, and ZooKeeper.
 * 5. We support both flow-style tasks and quartz-style jobs, and execution-dependency between tasks is also supported.
 * 6. Concept of "domain" is introduced to isolate different cluster of TaskTrackers. so TaskTrackers with
 * 		same domain are homogeneous, other wise heterogeneous.
 * 
 * This project is inspired by some great open-source projects, like dubbo, hadoop, etc. Thanks to the open source communities.
 * 
 * @author jingyuan.sun
 */
public class JobTracker extends CompositeService {
	public static final String SERVER_NAME	= "zues_light_schedule";
	
	protected static DomainLogger L = DomainLogger.getLogger(JobTracker.class.getSimpleName());
	
	/* status of each domain */
	public static enum DomainStatus {
		Init,		// initial state, not initialized
		Prepared,	// initialized, and waiting for registration from former work nodes.
		Running,	// ready
		Abandon,	// frozen to be deleted
		
		Pause,		// disable new units adding into scheduler
		Stop,		// neither adding units into scheduler nor accept units
	}
	
	private HttpFrontService frontService;
	
	private TaskProcessor tProcessor;
	
	private NodeProcessor nProcessor;
	
	private JobStoreFactory jobstoreFactory;
	
	private AppContext appContext;
	
	public JobTracker(String name) {
		super(name);
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		appContext = new AppContextImpl();
		appContext.setConfig(conf);
		appContext.setHAStatus(HAStatus.UNKNOWN);
		
		QuartzTaskManager.init(appContext);
		
		// job store
		jobstoreFactory = JobStoreHelper.getJobStoreFactory(conf.get("store.url"));
		JobStore jobstore = jobstoreFactory.getJobStore(conf);
		appContext.setJobStore(jobstore);
		L.info(DomainLogger.SYS, "inited jobstore -> " + jobstore.getClass().getSimpleName());
		
		// registry
		RegistryConfig registryConfig = RegistryConfig.readConfig(conf);
		MasterRegistry registry = RegistryFactory.getMasterRegistry(registryConfig);
		if (registry == null)
			throw new ZslsRuntimeException("failed to get registry");
		appContext.setMasterRegistry(registry);
		L.info(DomainLogger.SYS, "inited registry addr: " + registryConfig.getConnAddr());
		
		// node meta store
		NodeMetaStore nodeMetaStore = NodeMetaStoreFactory.getMetaStore();
		appContext.setNodeMetaStore(nodeMetaStore);
		addService(nodeMetaStore);
		
		// domain manager
		DomainManager domainManager = new DomainManager();
		appContext.setDomainManager(domainManager);
		
		// dispatchers
		Dispatcher taskDispatcher = new HubDispatcher();
		appContext.setTaskDispatcher(taskDispatcher);
		addIfService(taskDispatcher);
		
		Dispatcher normDispatcher = new AsyncDispatcher();
		appContext.setTrackDispatcher(normDispatcher);
		addIfService(normDispatcher);
		L.info(DomainLogger.SYS, "inited task dispatcher and norm dispatcher ");
		
		// node manager
		WorkerManager nodeManager = new WorkerManager(); 
		appContext.setNodeManager(nodeManager);
		
		// timeout manager
		TimeoutManager timeoutManager = new TimeoutManager();
		appContext.setTimeoutManager(timeoutManager);
		addService(timeoutManager);
		
		// norm schedulers and task processor
		tProcessor = new TaskProcessor(appContext);
		appContext.setTaskProcessor(tProcessor);
		appContext.getTaskDispatcher().register(TaskEventType.class, tProcessor);
		addService(tProcessor);
		L.info(DomainLogger.SYS, "inited task processor");
	
		// quartz scheduler
		QuartzScheduler qscheduler = new QuartzScheduler();
		appContext.setQuartzScheduler(qscheduler);
		addService(qscheduler);
		
		// node processor
		nProcessor = new NodeProcessor(appContext);
		appContext.getTrackDispatcher().register(NodeEventType.class, nProcessor);
		L.info(DomainLogger.SYS, "inited node processor");
		
		// HA service
		MasterMonitor masterMonitor = new MasterMonitor(appContext);
		addService(masterMonitor);
		appContext.getTrackDispatcher().register(NotifyEvent.class, masterMonitor);
		
		// node liveness monitor
		WNodeLiveMonitor nodeMonitor = new WNodeLiveMonitor(appContext);
		appContext.setNodeLiveMonitor(nodeMonitor);
		addService(nodeMonitor);
		
		// inner_tracker service
		InnerTrackService innerService = new InnerTrackService(appContext, nodeMonitor);
		addService(innerService);
		L.info(DomainLogger.SYS, "inited inner track service");
		
		// front server
		frontService = new HttpFrontService(appContext);
		addService(frontService);
		L.info(DomainLogger.SYS, "inited front server");
		
		super.serviceInit(conf);
	}
	
	protected void serviceStart() throws Exception {
		super.serviceStart();
	}
	
	public static void main(String[] args) throws Exception {
		JobTracker server = new JobTracker(SERVER_NAME);
		Configuration conf = new Configuration().load();
		server.init(conf);
		server.start();
	}

}
