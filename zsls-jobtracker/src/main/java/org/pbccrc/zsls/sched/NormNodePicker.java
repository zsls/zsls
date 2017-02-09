package org.pbccrc.zsls.sched;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.nodes.WorkNode;
import org.pbccrc.zsls.nodes.WorkerManager;

public class NormNodePicker implements NodePicker {
	
	@Override
	public WorkNode nextNode(WorkerManager manager, String domain, int retry) {
		Map<NodeId, WorkNode> nodes = manager.getNodes(domain);
		WorkNode node = null;
		int min = Integer.MAX_VALUE;
		if (nodes != null) {
			if (retry == 0) {
				for (WorkNode n : nodes.values()) {
					if (!n.isRegistered()) 	continue;
					if (n.isDisabled()) 	continue;
					if (!n.isFullyOccupied() && n.getRunningTasks().size() < min) {
						node = n;
						min = n.getRunningTasks().size();
					}
				}
			}
			else {
					
			}
		}
		return node;
	}

	
	private WorkComparator comparator = new WorkComparator();
	@Override
	public List<WorkNode> getAssignableNodes(WorkerManager manager, String domain) {
		List<WorkNode> list = new ArrayList<WorkNode>();
		Map<NodeId, WorkNode> nodes = manager.getNodes(domain);
		if (nodes != null) {
			for (WorkNode n : nodes.values()) {
				if (n.isRegistered() && !n.isDisabled() && !n.isFullyOccupied())
					list.add(n);
			}
		}
		Collections.sort(list, comparator);
		return list;
	}
	
	
	public static class WorkComparator implements Comparator<WorkNode> {
		@Override
		public int compare(WorkNode o1, WorkNode o2) {
			if (o1.getRunningTasks().size() < o2.getRunningTasks().size())
				return -1;
			else if (o1.getRunningTasks().size() > o2.getRunningTasks().size())
				return 1;
			return 0;
		}
	}

}
