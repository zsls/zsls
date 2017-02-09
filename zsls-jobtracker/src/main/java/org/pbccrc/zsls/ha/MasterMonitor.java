package org.pbccrc.zsls.ha;

import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.JobTracker.DomainStatus;
import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.domain.DomainInfo;
import org.pbccrc.zsls.domain.DomainManager;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.eventdispatch.EventHandler;
import org.pbccrc.zsls.exception.ZslsRuntimeException;
import org.pbccrc.zsls.nodes.NodeIdInfo;
import org.pbccrc.zsls.nodes.WorkNode;
import org.pbccrc.zsls.nodes.WorkerManager;
import org.pbccrc.zsls.nodes.store.NodeMeta;
import org.pbccrc.zsls.nodes.store.NodeMetaStore;
import org.pbccrc.zsls.registry.MasterRegistry;
import org.pbccrc.zsls.registry.NotifyEvent;
import org.pbccrc.zsls.registry.NotifyListener;
import org.pbccrc.zsls.registry.RegNode;
import org.pbccrc.zsls.registry.RegistryStateListener;
import org.pbccrc.zsls.service.AbstractService;
import org.pbccrc.zsls.tasks.TaskEvent;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.RegisterUtils;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;
import org.pbccrc.zsls.utils.timeout.Expirable;
import org.pbccrc.zsls.utils.timeout.RealtimeExpireItem;
import org.pbccrc.zsls.utils.timeout.TimeoutHandler;
import org.quartz.SchedulerException;

public class MasterMonitor extends AbstractService implements 
			NotifyListener, EventHandler<MasterEvent>, RegistryStateListener {
	
	protected static DomainLogger L = DomainLogger.getLogger(MasterMonitor.class.getSimpleName());
	
	private AppContext context;
	
	private MasterMonitor(String name) {
		super(name);
	}
	
	public MasterMonitor(AppContext context) {
		super(MasterMonitor.class.getSimpleName());
		this.context = context;
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		context.getRegistry().addServiceListener(this);
		context.getTimeoutManager().registerHandler(ZslsConstants.TIMEOUT_LOAD_REGISTER, 
						new LoadRegisterTimeoutListener());
		super.serviceInit(conf);
	}
	
	protected void serviceStart() throws Exception {
		super.serviceStart();
		context.getRegistry().substribe(this);
		L.info(DomainLogger.SYS, "try to become master...");
		tryBecomeMaster();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void notify(NotifyEvent e, List<RegNode> nodes) {
		if (nodes.size() > 1) {
			L.error(null, "multiple RegNodes of NotifyEvent for masterRegistry");
			return;
		}
		L.debug(null, "notify event from registry: " + e);
		RegNode node = nodes.size() > 0 ? nodes.get(0) : null;
		MasterEvent event = new MasterEvent(e, node);
		context.getTrackDispatcher().getEventHandler().handle(event);
	}

	@Override
	public void handle(MasterEvent event) {
		switch (event.getType()) {
		case ADD:
			break;
		case REMOVE:
			L.error(DomainLogger.SYS, "master down, try register to be master");
			tryBecomeMaster();
			break;
		}
	}
	
	private synchronized void tryBecomeMaster() {
		if (context.getHAStatus() == HAStatus.MASTER) {
			L.info(DomainLogger.SYS, "already master now");
			return;
		}
		MasterRegistry registry = context.getRegistry();
		try {
			RegNode node = RegisterUtils.getLocalRegNode(context);
			if (registry.registerMaster(node)) {
				// init domains & nodes
				initDomainsAndNodes();
				
				// init RT & DT jobs
				for (String domain : context.getDomainManager().getRTDomains())
					context.getTaskProcessor().maybeTryLoadRTJobs(domain);
				context.getTaskProcessor().tryLoadDTJobs();
				
				// start collecting runtime info
				context.setHAStatus(HAStatus.MASTER);
				L.info(DomainLogger.SYS, "registered as master");
				
				int timeout = context.getConfig().getInt(ZslsConstants.LOAD_REGISTER_EXPIRE
						, ZslsConstants.DEFAULT_LOAD_REGISTER_EXPIRE);
				context.getTimeoutManager().add(new RealtimeExpireItem(timeout), 
						ZslsConstants.TIMEOUT_LOAD_REGISTER);
			} else {
				context.setHAStatus(HAStatus.STANDBY);
				node = registry.getMaster();
				L.info(DomainLogger.SYS, "registered as standby, current master: " + node);
			}
		} catch (Exception e) {
			e.printStackTrace();
			L.error(DomainLogger.SYS, "exception when registering master: " + e);
		}
	}
	
	private void initDomainsAndNodes() {
		NodeMetaStore nmstore = context.getNodeMetaStore();
		Map<DomainInfo, List<NodeMeta>> metas = nmstore.getAllNodeMetas();
		WorkerManager manager = context.getNodeManager();
		DomainManager dmanager = context.getDomainManager();
		for (DomainInfo info : metas.keySet()) {
			String domain = info.name;
			if (info.type == DomainType.DT) {
				dmanager.addDTDomain(domain);
			} else {
				if (!context.getJobStore().initForDomain(domain))
					throw new ZslsRuntimeException("JobStore failed to init domain " + domain);
				dmanager.addRTDomain(domain);
				context.getTaskProcessor().addRTDomain(domain);
			}
			List<NodeMeta> nodes = metas.get(info);
			if (nodes.size() == 0) {
				dmanager.changeDomainStatus(domain, DomainStatus.Running);
			}
			else {
				dmanager.changeDomainStatus(domain, DomainStatus.Prepared);
				for (NodeMeta m : nodes) {
					WorkNode node = new WorkNode(domain, m.nodeId, context, m.maxTaskNum, false);
					manager.addWorkNode(domain, node);
					context.getNodeLiveMonitor().register(new NodeIdInfo(m.nodeId, domain));
				}
			}
		}
		// add default DT domain if it's not existed
		String defaultDm = ZslsConstants.DEFAULT_DOMAIN;
		if (dmanager.getDomainStatus(defaultDm) == null) {
			if (!nmstore.addDomain(defaultDm, DomainType.DT))
				throw new ZslsRuntimeException("NodeMetaStore failed to add default domain");
			dmanager.addDTDomain(defaultDm);
			dmanager.changeDomainStatus(defaultDm, DomainStatus.Running);
		}
		L.info(DomainLogger.SYS, dmanager.dumpDomains());
	}
	
	private class LoadRegisterTimeoutListener implements TimeoutHandler {
		@SuppressWarnings("unchecked")
		@Override
		public void handleTimeout(Expirable item) {
			DomainManager manager = context.getDomainManager();
			boolean dtOK = manager.isAllDTDomainsReady();
			for (DomainInfo info : manager.getAllDomainInfos()) {
				String domain = info.name;
				DomainStatus stat = info.status;
				if (stat == DomainStatus.Prepared) {
					StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
					b.append("stop waiting for unregistered nodes -> [");
					List<NodeId> unregistered = context.getNodeManager().getUnregisteredNodes(domain);
					for (NodeId id : unregistered) {
						b.append(id).append(", ");
					}
					b.append("]");
					L.warn(domain, b.toString());
					manager.changeDomainStatus(domain, DomainStatus.Running);
					L.info(domain, "domain status -> " + DomainStatus.Running);
					DomainType dtype = manager.getDomainType(domain);
					if (dtype == DomainType.RT) {
						TaskEvent event = TaskEvent.getTriggerEvent(domain, dtype);
						context.getTaskDispatcher().getEventHandler().handle(event);
					}
				}
			}
			if (!dtOK) {
				try {
					context.getQuartzScheduler().scheduleJobs();
				} catch (SchedulerException e) {
					L.fatal(ZslsConstants.FAKE_DOMAIN_DT, " exception when scheduling DT jobs !!! ");
					e.printStackTrace();
					System.exit(-1);
				}
				TaskEvent event = TaskEvent.getTriggerEvent(ZslsConstants.FAKE_DOMAIN_DT, DomainType.DT);
				context.getTaskDispatcher().getEventHandler().handle(event);	
			}
		}
	}

	@Override
	public void serviceEnabled() {
		L.info(null, "registry service enabled, try register master...");
		tryBecomeMaster();
	}

	@Override
	public void serviceDisabled() {
		L.error(null, "registry service disabled");
		context.setHAStatus(HAStatus.UNKNOWN);
	}

}
