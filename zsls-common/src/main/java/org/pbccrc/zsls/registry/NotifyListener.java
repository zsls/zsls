package org.pbccrc.zsls.registry;

import java.util.List;

public interface NotifyListener {
	
	void notify(NotifyEvent event, List<RegNode> nodes);

}
