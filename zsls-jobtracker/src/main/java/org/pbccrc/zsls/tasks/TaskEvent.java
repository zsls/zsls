package org.pbccrc.zsls.tasks;

import java.util.List;

import org.pbccrc.zsls.api.thrift.records.TTaskId;
import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.eventdispatch.AbstractEvent;
import org.pbccrc.zsls.front.request.QRequest;
import org.pbccrc.zsls.nodes.WorkNode;
import org.pbccrc.zsls.tasks.dt.QuartzTaskInfo;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;

public class TaskEvent extends AbstractEvent<TaskEventType> {
	
	// required
	private String domain;
	private DomainType dtype;
	
	/* for newly accepted unit */
	private RTJobFlow unit;
	
	/* for feedback task events coming from work nodes, and task timeout */
	private TaskResult result;
	
	/* for new quartz task */
	private QuartzTaskInfo quartzTask;
	
	/* for redo task */
	private QRequest request;
	
	/* for kill-job command */
	private String jobId;
	
	public static TaskEvent getKillJobEvent(DomainType dtype, String jobId) {
		TaskEvent event = new TaskEvent(TaskEventType.KILL_JOB, null, dtype);
		event.jobId = jobId;
		return event;
	}
	
	public static TaskEvent getResumeJobEvent(DomainType dtype, String jobId) {
		TaskEvent event = new TaskEvent(TaskEventType.RESUME_JOB, null, dtype);
		event.jobId = jobId;
		return event;
	}
	
	public static TaskEvent getRTNewJobEvent(String domain, RTJobFlow unit) {
		TaskEvent event = new TaskEvent(TaskEventType.RT_NEW_JOB, domain, DomainType.RT);
		event.unit = unit;
		return event;
	}
	
	public static TaskEvent getDTNewJobEvent(QuartzTaskInfo task) {
		TaskEvent event = new TaskEvent(TaskEventType.DT_NEW_JOB, null, DomainType.DT);
		event.quartzTask = task;
		return event;
	}
	
	public static TaskEvent getTaskResponseEvent(String domain, DomainType dtype, 
			TaskResult result, List<TTaskId> runningTasks) {
		TaskEventType type = result.getAction() == TaskAction.COMPLETE ? 
				TaskEventType.COMPLETE : TaskEventType.FAIL;
		TaskEvent event = new TaskEvent(type, domain, dtype);
		event.result = result;
		event.tasks = runningTasks;
		return event;
	}
	
	public static TaskEvent getTriggerEvent(String domain, DomainType dtype) {
		TaskEventType type = dtype == DomainType.DT ? 
				TaskEventType.DT_TRIGGER : TaskEventType.RT_TRIGGER;
		TaskEvent event = new TaskEvent(type, domain, dtype);
		return event;
	}
	
	public static TaskEvent getNewNodeEvent(String domain, DomainType dtype) {
		if (dtype == DomainType.DT)
			throw new IllegalArgumentException("NEW_NODE event not supported for DT domains");
		TaskEvent event = new TaskEvent(TaskEventType.RT_NEW_NODE, domain, dtype);
		return event;
	}
	
	public static TaskEvent getRedoTaskEvent(QRequest request) {
		TaskEvent event = new TaskEvent(TaskEventType.REDO_TASK, 
				request.getUserRequest().getDomain(), DomainType.RT);
		event.request =  request;
		return event;
	}
	
	private WorkNode node;
	private List<TTaskId> tasks;
	public static TaskEvent getUpdateRunningEvent(String domain, DomainType dtype,
			WorkNode node, List<TTaskId> tasks) {
		TaskEvent event = new TaskEvent(TaskEventType.UPDATE_RUNNING, domain, dtype);
		event.node = node;
		event.tasks = tasks;
		return event;
	}
	
	
	// constructor
	private TaskEvent(TaskEventType type, String domain, DomainType dtype) {
		super(type);
		this.domain = domain;
		this.dtype = dtype;
	}

	// public methods
	public RTJobFlow getUnit() {
		return unit;
	}

	public String getDomain() {
		return domain;
	}
	
	public DomainType getDomainType() {
		return dtype;
	}

	public TaskResult getResult() {
		return result;
	}

	public QuartzTaskInfo getQuartzTaskInfo() {
		return quartzTask;
	}

	public QRequest getRequest() {
		return request;
	}
	
	public String getJobId() {
		return jobId;
	}
	
	public void setRequest(QRequest request) {
		this.request = request;
	}
	

	public WorkNode getNode() {
		return node;
	}

	public List<TTaskId> getTasks() {
		return tasks;
	}

	// private methods
	protected void setDtype(DomainType dtype) {
		this.dtype = dtype;
	}

	protected void setQuartzTask(QuartzTaskInfo quartzTask) {
		this.quartzTask = quartzTask;
	}

	protected void setJobId(String jobId) {
		this.jobId = jobId;
	}

	protected void setDomain(String domain) {
		this.domain = domain;
	}


	protected void setUnit(RTJobFlow unit) {
		this.unit = unit;
	}


	protected void setResult(TaskResult result) {
		this.result = result;
	}

}
