package org.pbccrc.zsls.nodes;

import java.util.Set;

import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.eventdispatch.EventHandler;
import org.pbccrc.zsls.jobengine.JobEngine;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.tasks.LocalJobManager;
import org.pbccrc.zsls.tasks.TaskEvent;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;

public class NodeProcessor implements EventHandler<NodeEvent>{
	private static DomainLogger L = DomainLogger.getLogger(NodeProcessor.class.getSimpleName());
	
	private AppContext context;
	
	public NodeProcessor(AppContext context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handle(NodeEvent event) {
		NodeId id = event.getNodeId();
		if (event.getType() == NodeEventType.LOST) {
			String domain = event.getDomain();
			DomainType dtype = context.getDomainManager().getDomainType(domain);
			L.warn(domain, "work node " + id + " fail!");
			WorkerManager manager = context.getNodeManager();
			WorkNode node = manager.getNode(domain, id);
			if (node == null) {
				L.error(domain, "failed node " + id + " unregistered");
			}
			else {
				Set<TaskId> tasks = node.getRunningTasks();
				if (domain == ZslsConstants.DEFAULT_DOMAIN) {
					L.warn(domain, "would not re-assign " + tasks.size() + " tasks");
				}
				else if (tasks.size() > 0) {
					StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
					b.append("re-assign ").append(tasks.size()).append(" tasks: [");
					for (TaskId t : tasks) {
						b.append(t);
						JobEngine engine = context.getTaskProcessor().getJobEngine(domain, dtype);
						Task task = LocalJobManager.getJobManager(domain, dtype).getTask(t.id);
						if (task != null) {
							b.append("(ok) ");
							task.markStatus(TaskStat.Init);
							context.getTimeoutManager().cancelTimeout(task);
							engine.addToExecutableQueue(task);
						} else {
							b.append("(miss) ");
						}
					}
					b.append("]");
					L.info(domain, b.toString());	
				}
				manager.removeWorkNode(event.getDomain(), event.getNodeId());
				TaskEvent taskEvent = TaskEvent.getTriggerEvent(domain, dtype);
				context.getTaskDispatcher().getEventHandler().handle(taskEvent);
			}
		}
	}

}
