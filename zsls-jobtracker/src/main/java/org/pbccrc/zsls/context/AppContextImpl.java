package org.pbccrc.zsls.context;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.domain.DomainManager;
import org.pbccrc.zsls.eventdispatch.Dispatcher;
import org.pbccrc.zsls.ha.HAStatus;
import org.pbccrc.zsls.jobstore.JobStore;
import org.pbccrc.zsls.nodes.WNodeLiveMonitor;
import org.pbccrc.zsls.nodes.WorkerManager;
import org.pbccrc.zsls.nodes.store.NodeMetaStore;
import org.pbccrc.zsls.registry.MasterRegistry;
import org.pbccrc.zsls.sched.QuartzScheduler;
import org.pbccrc.zsls.tasks.TaskProcessor;
import org.pbccrc.zsls.utils.timeout.TimeoutManager;

public class AppContextImpl implements AppContext {

	private DomainManager domainManager;
	private JobStore jobstore;
	private Dispatcher taskDispatcher;
	private Dispatcher normDispatcher;
	private Configuration config;
	private TaskProcessor processor;
	private WorkerManager nodeManager;
	private NodeMetaStore nodeMetaStore;
	private MasterRegistry registry;
	private HAStatus status;
	private TimeoutManager timeoutManager;
	private WNodeLiveMonitor nodeliveMonitor;
	private QuartzScheduler quartzScheduler;
	
	
	@Override
	public DomainManager getDomainManager() {
		return domainManager;
	}

	@Override
	public void setDomainManager(DomainManager manager) {
		this.domainManager = manager;
	}

	@Override
	public JobStore getJobStore() {
		return jobstore;
	}

	@Override
	public void setJobStore(JobStore store) {
		this.jobstore = store;
	}

	@Override
	public Dispatcher getTaskDispatcher() {
		return taskDispatcher;
	}

	@Override
	public void setTaskDispatcher(Dispatcher dispatcher) {
		this.taskDispatcher = dispatcher;
	}

	@Override
	public Dispatcher getTrackDispatcher() {
		return normDispatcher;
	}

	@Override
	public void setTrackDispatcher(Dispatcher dispatcher) {
		this.normDispatcher = dispatcher;
	}

	@Override
	public Configuration getConfig() {
		return config;
	}

	@Override
	public void setConfig(Configuration conf) {
		this.config = conf;
	}

	@Override
	public HAStatus getHAStatus() {
		return status;
	}

	@Override
	public void setHAStatus(HAStatus status) {
		this.status = status;
	}

	@Override
	public MasterRegistry getRegistry() {
		return registry;
	}


	@Override
	public void setMasterRegistry(MasterRegistry registry) {
		this.registry = registry;
	}

	@Override
	public WorkerManager getNodeManager() {
		return nodeManager;
	}

	@Override
	public void setNodeManager(WorkerManager manager) {
		this.nodeManager = manager;
	}

	@Override
	public TaskProcessor getTaskProcessor() {
		return processor;
	}

	@Override
	public void setTaskProcessor(TaskProcessor processor) {
		this.processor = processor;
	}

	@Override
	public NodeMetaStore getNodeMetaStore() {
		return nodeMetaStore;
	}

	@Override
	public void setNodeMetaStore(NodeMetaStore store) {
		this.nodeMetaStore = store;
	}

	@Override
	public TimeoutManager getTimeoutManager() {
		return timeoutManager;
	}

	@Override
	public void setTimeoutManager(TimeoutManager manager) {
		this.timeoutManager = manager;
	}

	@Override
	public WNodeLiveMonitor getNodeLiveMonitor() {
		return nodeliveMonitor;
	}

	@Override
	public void setNodeLiveMonitor(WNodeLiveMonitor monitor) {
		this.nodeliveMonitor = monitor;
	}

	@Override
	public QuartzScheduler getQuartzScheduler() {
		return quartzScheduler;
	}

	@Override
	public void setQuartzScheduler(QuartzScheduler scheduler) {
		this.quartzScheduler = scheduler;
	}

}
