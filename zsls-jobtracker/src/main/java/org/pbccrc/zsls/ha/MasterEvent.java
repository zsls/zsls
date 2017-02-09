package org.pbccrc.zsls.ha;

import org.pbccrc.zsls.eventdispatch.AbstractEvent;
import org.pbccrc.zsls.registry.NotifyEvent;
import org.pbccrc.zsls.registry.RegNode;

public class MasterEvent extends AbstractEvent<NotifyEvent> {
	
	private RegNode node;

	private MasterEvent(NotifyEvent type) {
		super(type);
	}
	
	public MasterEvent(NotifyEvent type, RegNode node) {
		super(type);
		this.node = node;
	}
	
	public RegNode getNode() {
		return node;
	}

}
