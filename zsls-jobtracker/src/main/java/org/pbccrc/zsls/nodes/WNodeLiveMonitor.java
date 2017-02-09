package org.pbccrc.zsls.nodes;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.utils.clock.SystemClock;
import org.pbccrc.zsls.utils.core.AbstractLivelinessMonitor;

public class WNodeLiveMonitor extends AbstractLivelinessMonitor<NodeIdInfo> {
	
	private AppContext context;
	
	public WNodeLiveMonitor(AppContext context) {
		super(WNodeLiveMonitor.class.getSimpleName(), new SystemClock());
		this.context = context;
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		int expireIntvl = conf.getInt(ZslsConstants.RM_NM_EXPIRY_INTERVAL_MS,
            ZslsConstants.DEFAULT_WN_EXPIRY_INTERVAL_MS);
		setExpireInterval(expireIntvl);
		setMonitorInterval(5000);	// 5s
		super.serviceInit(conf);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void expire(NodeIdInfo ob) {
		context.getNodeMetaStore().removeNode(ob.getDomain(), ob.getId());
		context.getTrackDispatcher().getEventHandler()
			.handle(new NodeEvent(NodeEventType.LOST, ob.getId(), ob.getDomain()));
	}

}
