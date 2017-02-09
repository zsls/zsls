package org.pbccrc.zsls.sched;

import java.util.List;

import org.pbccrc.zsls.nodes.WorkNode;
import org.pbccrc.zsls.nodes.WorkerManager;

public interface NodePicker {
	
	WorkNode nextNode(WorkerManager manager, String domain, int retry);
	
	List<WorkNode> getAssignableNodes(WorkerManager manager, String domain);

}
