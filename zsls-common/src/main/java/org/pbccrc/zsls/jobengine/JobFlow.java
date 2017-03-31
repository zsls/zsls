package org.pbccrc.zsls.jobengine;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import org.pbccrc.zsls.jobengine.bpmn.DataFlow;
import org.pbccrc.zsls.jobengine.bpmn.FlowObj;
import org.pbccrc.zsls.jobengine.bpmn.FlowObjImpl;
import org.pbccrc.zsls.jobengine.statement.ConditionExp;

public class JobFlow {
	
	public JobFlow() {
		start = new TempFlowObj();
		end = new TempFlowObj();
	}
	public JobFlow(JobId id) {
		this();
		this.jobId = id;
	}
	
	protected TempFlowObj start;
	public FlowObj getStartObj() {
		return start;
	}
	
	protected TempFlowObj end;
	public boolean isFinished() {
		return end.executed;
	}
	public void markJobFinish(boolean finished) {
		end.executed = finished;
	}
	
	protected JobId jobId;
	public JobId getJobId() {
		return jobId;
	}
	public void setJobId(JobId jobId) {
		this.jobId = jobId;
	}
	
	protected String domain;
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	protected String swiftNum;
	public String getSwiftNum() {
		return this.swiftNum;
	}
	public void setSwiftNum(String swiftNum) {
		this.swiftNum = swiftNum;
	}
	
	protected Date generateTime;
	public Date getGenerateTime() {
		return generateTime;
	}
	public void setGenerateTime(Date generateTime) {
		this.generateTime = generateTime;
	}

	protected TaskCollector collector;
	public Iterator<Task> getTaskIterator() {
		if (collector == null) {
			collector = new TaskCollector();
			collector.visit(this);
		}
		return collector.tasks.iterator();
	}
	public HashSet<Task> getTasks() {
		if (collector == null) {
			collector = new TaskCollector();
			collector.visit(this);
		}
		return collector.tasks;
	}
	
	protected int taskNum = -1;
	public int getTaskNum() {
		if (taskNum < 0) {
			taskNum = getTasks().size();
		}
		return taskNum;
	}
	
	
	public Task getTask(String taskId) {
		Iterator<Task> it = getTaskIterator();
		while (it.hasNext()) {
			Task task = it.next();
			if (task.getTaskId().equals(taskId))
				return task;
		}
		return null;
	}
	
	public void addHeadFlowObj(FlowObj obj) {
		DataFlow f = new DataFlow(start, obj, null);
		f.takeEffect();
	}
	
	public void addTailFlowObj(FlowObj obj, ConditionExp condition) {
		DataFlow f = new DataFlow(obj, end, condition);
		f.takeEffect();
	}
	
	
	/******----------------------******/

	public static class TaskCollector {
		HashSet<Task> tasks = new HashSet<Task>();
		public void visit(JobFlow unit) {
			for (DataFlow flow : unit.start.getOutFlows()) {
				FlowObj obj = flow.getTarget();
				visit(obj);
			}
		}
		private void visit(FlowObj obj) {
			if (tasks.contains(obj))
				return;
			if (obj instanceof Task) {
				tasks.add((Task)obj);
			} 
			for (DataFlow flow : obj.getOutFlows())
				visit(flow.getTarget());
		}
	}

	public static class TempFlowObj extends FlowObjImpl {
		boolean executed;
		public boolean isExecuted() {
			return executed;
		}
	}

}
