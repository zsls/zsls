package org.pbccrc.zsls.jobengine;

import java.util.TreeSet;

import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.jobengine.JobFlow.TempFlowObj;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.jobengine.bpmn.ConvergeGateway;
import org.pbccrc.zsls.jobengine.bpmn.DataFlow;
import org.pbccrc.zsls.jobengine.bpmn.DivergeGateway;
import org.pbccrc.zsls.jobengine.bpmn.FlowObj;
import org.pbccrc.zsls.jobengine.statement.Param;
import org.pbccrc.zsls.utils.DomainLogger;

public class JobEngine {
	private static DomainLogger L = DomainLogger.getLogger(JobEngine.class.getSimpleName());
	
	protected TreeSet<Task> executableQueue;
	protected JobManager manager;
	
	public JobEngine(JobManager manager) {
		this.manager = manager;
		executableQueue = new TreeSet<Task>();
	}
	
	
	/**************** public interface start ***************/
	
	public int feed(JobFlow unit) {
		if (unit.isFinished())
			return 0;
		if (!manager.register(unit))
			return -1;
		int num = 0;
		for (DataFlow flow : unit.getStartObj().getOutFlows()) {
			num += tryAdd(flow, null, num);
		}
		if (unit.isFinished())
			manager.unregister(unit);
		return num;
	}
	
	public JobFlow complete(TaskId id, TaskResult ret) {
		Task task = manager.getTask(id.id);
		if (task == null) {
			L.error(DomainLogger.SYS, "completed task " + id + " not registered");
			return null;
		}
		
		// update status and result message
		Param p = Param.getParam(ret);
		task.updateExecuteResult(ret.getKeyMessage(), ret.getAppendInfo());
		if (ret.getAction() == TaskAction.COMPLETE)
			task.markStatus(TaskStat.Finished);
		else
			task.markStatus(TaskStat.Fail);
		
		for (DataFlow flow : task.getOutFlows())
			tryAdd(flow, p, 0);
		
		JobFlow unit = task.getJobFlow();
		if (unit.isFinished()) {
			manager.unregister(unit);
			return unit;
		}
		return null;
	}
	
	public Task next() {
		synchronized (executableQueue) {
			return executableQueue.pollFirst();
		}
	}
	
	public void addToExecutableQueue(Task task) {
		synchronized (executableQueue) {
			executableQueue.add(task);
		}
	}
	
	public void removeFromExecutableQueue(String taskId) {
		Task task = manager.getTask(taskId);
		if (task != null)
			synchronized (executableQueue) {
			executableQueue.remove(task);
		}
	}
	
	public int getExecutableNum() {
		return executableQueue.size();
	}
	
	/**************** public interface end ***************/
	
	
	private void inActivePath(DataFlow flow) {
		if (!(flow.getSource() instanceof DivergeGateway))
			return;
		int gateNum = 1;
		DataFlow curFlow = flow;
		while (curFlow != null) {
			if (curFlow.getTarget() instanceof DivergeGateway) {
				curFlow = curFlow.getTarget().getOutFlows().get(0);
				gateNum ++;
				continue;
			}
			else if (curFlow.getTarget() instanceof ConvergeGateway) {
				if (--gateNum == 0)
					break;
			}
			curFlow = curFlow.getTarget().getOutFlows().size() > 0 ?
					curFlow.getTarget().getOutFlows().get(0) : null;
		}
		if (curFlow != null) {
			curFlow.setActive(false);
		}
	}
	
	protected int tryAdd(DataFlow flow, Param p, int num) {
		if (!flow.isConditionMet(p)) {
			// inactive this path
			if (flow.getSource() instanceof DivergeGateway) {
				inActivePath(flow);
			}
			return num;
		}
		int ret = num;
		FlowObj obj = flow.getTarget();
		if (obj instanceof Task) {
			Task task = (Task)obj;
			TaskStat stat = task.getStatus();
			if (stat == TaskStat.Finished) {
				Param param = Param.getParam(task.getResultMsg(), true);
				for (DataFlow f : task.getOutFlows())
					ret = tryAdd(f, param, num);
			} else if (stat == TaskStat.Fail) {
				Param param = Param.getParam(task.getResultMsg(), false);
				for (DataFlow f : task.getOutFlows())
					ret = tryAdd(f, param, num);
			} else {
				synchronized (executableQueue) {
					executableQueue.add(task);
				}
				ret ++;	
			}
		}
		else if (obj instanceof DivergeGateway) {
			DivergeGateway gw = (DivergeGateway)obj;
			for (DataFlow f : gw.getOutFlows())
				ret = tryAdd(f, p, num);
		}
		else if (obj instanceof ConvergeGateway) {
			ConvergeGateway gw = (ConvergeGateway)obj;
			if (gw.converge(flow)) {
				for (DataFlow f : gw.getOutFlows())
					ret = tryAdd(f, p, num);
			}
		}
		else if (obj instanceof TempFlowObj) {
			TempFlowObj end = (TempFlowObj)obj;
			end.executed = true;
		}
		return ret;
	}

}
