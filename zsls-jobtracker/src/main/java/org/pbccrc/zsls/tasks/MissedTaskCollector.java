package org.pbccrc.zsls.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.pbccrc.zsls.api.thrift.records.TTaskId;
import org.pbccrc.zsls.collection.Pair;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.nodes.WorkNode;

public class MissedTaskCollector {
	
	AppContext context;
	
	@SuppressWarnings("rawtypes")
	Map<String/*task id*/, Pair> monitorQueue;
	
	long checkInterval;
	long checkInvalidTime;
	
	@SuppressWarnings("rawtypes")
	public MissedTaskCollector(AppContext context) {
		this.context = context;
		this.monitorQueue = new HashMap<String, Pair>();
		this.checkInterval = 1000L;
		this.checkInvalidTime = context.getConfig().getInt(ZslsConstants.HEART_BEAT_INTERVAL, ZslsConstants.DEFAULT_HEART_BEAT_INTERVAL) * 2;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<TaskAssignInfo> checkRunningTask(String domain, WorkNode worker, List<TTaskId> runningTasks) {
		List<TaskAssignInfo> ret = new ArrayList<TaskAssignInfo>(3);
		DomainType dtype = context.getDomainManager().getDomainType(domain);
		long curtime = System.currentTimeMillis();
		
		HashSet<String> rptRunning = new HashSet<String>();
		if (runningTasks != null) {
			for (TTaskId tid : runningTasks) {
				monitorQueue.remove(tid.taskid);
				rptRunning.add(tid.taskid);
			}
		}
		
		if (curtime - worker.getLastCheckTaskTime() > checkInterval) {
			for (TaskId id : worker.getRunningTasks()) {
				if (!rptRunning.contains(id.id) && !monitorQueue.containsKey(id.id)) {
					TaskAssignInfo item = new TaskAssignInfo(domain, dtype, worker, id.id);
					monitorQueue.put(id.id, new Pair(item, curtime));
				}
			}
			for (Entry<String, Pair> e : monitorQueue.entrySet()) {
				TaskAssignInfo item = (TaskAssignInfo)e.getValue().getKey();
				long tmptime = (Long)e.getValue().getValue();
				if (curtime - tmptime > checkInvalidTime) {
					String taskid = e.getKey();
					if (worker.getNodeId().equals(item.node.getNodeId())) {
						ret.add(item);
						monitorQueue.remove(taskid);
					}
				}
			}
			worker.setLastCheckTaskTime(curtime);
		}
		return ret;
	}
	
	public static class TaskAssignInfo {
		public String domain;
		public DomainType dtype;
		public WorkNode node;
		public String taskId;
		private TaskAssignInfo(String domain, DomainType dtype, WorkNode node, String taskId) {
			this.domain = domain;
			this.dtype = dtype;
			this.node = node;
			this.taskId = taskId;
		}
	}

}
