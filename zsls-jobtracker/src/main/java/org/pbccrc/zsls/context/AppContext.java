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

public interface AppContext {
	
	/* status */
	HAStatus getHAStatus();
	void setHAStatus(HAStatus status);
	
	
	/* domain manager */
	DomainManager getDomainManager();
	void setDomainManager(DomainManager manager);
	
	
	/* registry */
	MasterRegistry getRegistry();
	void setMasterRegistry(MasterRegistry registry);
	
	
	/* task processor */
	TaskProcessor getTaskProcessor();
	void setTaskProcessor(TaskProcessor processor);
	
	QuartzScheduler getQuartzScheduler();
	void setQuartzScheduler(QuartzScheduler scheduler);
	
	
	/* work node manager */
	WorkerManager getNodeManager();
	void setNodeManager(WorkerManager manager);
	
	
	/* config */
	Configuration getConfig();
	void setConfig(Configuration conf);
	
	
	/* job store */
	JobStore getJobStore();
	void setJobStore(JobStore store);
	
	
	/* node meta store */
	NodeMetaStore getNodeMetaStore();
	void setNodeMetaStore(NodeMetaStore store);
	
	
	// task events, like task report, new task receive
	Dispatcher getTaskDispatcher();
	void setTaskDispatcher(Dispatcher dispatcher);
	
	// track events, like node state, registry change 
	Dispatcher getTrackDispatcher();
	void setTrackDispatcher(Dispatcher dispatcher);
	
	
	/* timeout manager */
	TimeoutManager getTimeoutManager();
	void setTimeoutManager(TimeoutManager manager);
	
	
	/* node liveness monitor */
	WNodeLiveMonitor getNodeLiveMonitor();
	void setNodeLiveMonitor(WNodeLiveMonitor monitor);
	
}
