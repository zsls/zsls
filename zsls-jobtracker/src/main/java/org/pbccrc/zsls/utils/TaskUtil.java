package org.pbccrc.zsls.utils;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.api.client.IConvergeGateway;
import org.pbccrc.zsls.api.client.IDataFlow;
import org.pbccrc.zsls.api.client.IDivergeGateway;
import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.IRetryStrategy;
import org.pbccrc.zsls.api.client.ITask;
import org.pbccrc.zsls.api.client.old.IReEntry;
import org.pbccrc.zsls.api.client.old.IRelation;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.old.IUserTask;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskInfo;
import org.pbccrc.zsls.exception.ZslsRuntimeException;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobengine.bpmn.ConvergeGateway;
import org.pbccrc.zsls.jobengine.bpmn.DataFlow;
import org.pbccrc.zsls.jobengine.bpmn.DivergeGateway;
import org.pbccrc.zsls.jobengine.bpmn.FlowObj;
import org.pbccrc.zsls.jobengine.statement.ConditionExp;
import org.pbccrc.zsls.jobengine.statement.ExpParser;
import org.pbccrc.zsls.tasks.dt.DTJobId;
import org.pbccrc.zsls.tasks.dt.QuartzTaskManager;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.tasks.rt.RTTask;

public class TaskUtil extends JsonSerilizer {
	public final static String JOB_START = "START";
	public final static String JOB_END = "END";
	private final static String DIV_SUFFIX = "_DIVERAGE";
	private final static String CONV_SUFFIX = "_CONVERAGE";
	
	private static void setupTask(Task task, ITask iT, JobFlow job, ServerQuartzJob sjob) {
		if (iT.retryOp != null && iT.retryOp.valid())
			task.setRetryOp(iT.retryOp);
		task.setTargetNode(iT.targetNode);
		task.setTimeout(iT.timeout);
		task.setJobFlow(job);

		Map<String, String> params = new HashMap<String, String>(iT.params);
		Map<String, String> taskMeta = sjob.getRuntimeParams(task.getTaskId());
		if (taskMeta != null) {
			for (String k : taskMeta.keySet()) {
				params.put(k, taskMeta.get(k));
			}
		}
		task.setParams(params);
	}
	public static JobFlow parseJobFlow(IJobFlow iJobFlow) {
		if (iJobFlow == null)
			return null;
		
		JobFlow result = new JobFlow();
		result.setJobId(new DTJobId(iJobFlow.id));
		
		ServerQuartzJob sjob = QuartzTaskManager.getInstance().getJob(iJobFlow.id);
		List<IDataFlow> dataFlowList = iJobFlow.dataFlows;
		List<Object> flowObjList = iJobFlow.flowObjs;
		
		Map<String, FlowObj> objMap = new HashMap<String, FlowObj>();
		Map<String, List<FlowObj>> taskSlice = new HashMap<String, List<FlowObj>>();
		try {
			for (Object o : flowObjList) {	
				if (o instanceof ITask) {
					ITask iT = (ITask)o;
					if (iT.domain == null && iT.targetNode == null) 
						throw new ZslsRuntimeException("domain and targetNode both null");
					String domain = iT.domain == null ? ZslsConstants.DEFAULT_DOMAIN : iT.domain;
					
					if (iT.partitions <= 1) {
						Task task = new Task(iT.id, domain);
						setupTask(task, iT, result, sjob);
						objMap.put(iT.id, task);
					}
					else {
						DivergeGateway diver = new DivergeGateway();
						ConvergeGateway conver = new ConvergeGateway(false);
						diver.setJobFlow(result);
						conver.setJobFlow(result);
						for (int j = 0; j < iT.partitions; j ++) {
							String taskId = iT.id + "_part" + j;
							Task task = new Task(taskId, domain);
							setupTask(task, iT, result, sjob);
							
							task.getParams().put(ZslsConstants.TASKP_SLICE_SERIAL, String.valueOf(j));
							task.getParams().put(ZslsConstants.TASKP_SLICE_Num, String.valueOf(iT.partitions));
							
							if (taskSlice.get(iT.id) == null)
								taskSlice.put(iT.id, new LinkedList<FlowObj>());
							taskSlice.get(iT.id).add(task);
						}
						objMap.put(iT.id + DIV_SUFFIX, diver);
						objMap.put(iT.id + CONV_SUFFIX, conver);
					}
				}
				else if (o instanceof IDivergeGateway) {
					IDivergeGateway iDiver = (IDivergeGateway)o;
					DivergeGateway diver = new DivergeGateway();
					diver.setJobFlow(result);
					
					objMap.put(iDiver.id, diver);
				}
				else if (o instanceof IConvergeGateway) {
					IConvergeGateway iConver = (IConvergeGateway)o;
					ConvergeGateway conver = new ConvergeGateway(iConver.selective);
					conver.setJobFlow(result);
					
					objMap.put(iConver.id, conver);
				} else
					throw new ZslsRuntimeException("no type matched");
			}
			
			for (IDataFlow iDF : dataFlowList) {
				boolean isJobStart = false, isJobEnd = false;
				FlowObj srcFlow = null, tarFlow = null;
				if (!iDF.source.equalsIgnoreCase(JOB_START)) {
					srcFlow = objMap.get(iDF.source);
					if (srcFlow == null)
						srcFlow = objMap.get(iDF.source + CONV_SUFFIX);
				}
				else
					isJobStart = true;
				if (!iDF.target.equalsIgnoreCase(JOB_END)) {
					tarFlow = objMap.get(iDF.target);
					if (tarFlow == null)
						tarFlow = objMap.get(iDF.target + DIV_SUFFIX);
				}
				else
					isJobEnd = true;
				
				if ((!isJobStart && srcFlow == null) || (!isJobEnd && tarFlow == null))
					throw new ZslsRuntimeException("no such element named " + (srcFlow == null ? iDF.source : iDF.target) + " defined");
				
				ConditionExp exp = null;
				if (iDF.condition != null)
					exp = ExpParser.parse(iDF.condition);
				
				List<FlowObj> taskSliceS = taskSlice.get(iDF.source);
				// initialize task's inner DataFlow 
				if (objMap.get(iDF.source) == null && taskSliceS != null) {
					FlowObj tarFlowInner = srcFlow;
					for (FlowObj flow : taskSliceS) {
						DataFlow d1 = new DataFlow(flow, tarFlowInner, exp);
						d1.takeEffect();
					}
				}
				
				List<FlowObj> taskSliceT = taskSlice.get(iDF.target);
				if (objMap.get(iDF.target) == null && taskSliceT != null) {
					FlowObj srcFlowInner = tarFlow;
					for (FlowObj flow : taskSliceT) {
						DataFlow d2 = new DataFlow(srcFlowInner, flow, exp);
						d2.takeEffect();
					}
				}
				
				if (!isJobStart && !isJobEnd) {
					DataFlow df = new DataFlow(srcFlow, tarFlow, exp);
					df.takeEffect();
				} else {
					if (isJobStart)
						result.addHeadFlowObj(tarFlow);
					else
						result.addTailFlowObj(srcFlow, exp);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return result;
	}
	
	public static RTJobFlow parseJobUnit(IScheduleUnit unit) {
		try {
			
		String domain = unit.domain;
		long timeout = unit.timeout;
		RTJobFlow job = new RTJobFlow();
		job.setPreUnit(RTJobId.fromString(unit.preUnit));
		job.setDomain(domain);
		job.setSwiftNum(unit.swiftNum);
		
		HashMap<String, Task> taskmap = new HashMap<String, Task>();
		List<DataFlow> flows = new LinkedList<DataFlow>();
		for (IRelation relation : unit.relations) {
			IReEntry e = relation.preTasks;
			IReEntry o = relation.postTasks;
			FlowObj start = null;
			FlowObj end = null;
			ConditionExp condition = null;
			if (e.tasks.size() > 1) {
				FlowObj conv = new ConvergeGateway(false);
				for (IUserTask t : e.tasks) {
					Task task = taskmap.get(t.id) != null ? taskmap.get(t.id) : new RTTask(t, domain);
					task.updateTimeoutIfNecessary(timeout);
					DataFlow flow = new DataFlow(task, conv, ConditionExp.EXP_CODE_OK);
					flow.takeEffect();
				}
				start = conv;
			}
			else {
				IUserTask t = e.tasks.get(0);
				Task task = taskmap.get(t.id) != null ? taskmap.get(t.id) : new RTTask(t, domain);
				task.updateTimeoutIfNecessary(timeout);
				start = task;
				condition = ConditionExp.EXP_CODE_OK;
			}
			if (o.tasks.size() > 1) {
				FlowObj div = new DivergeGateway();
				for (IUserTask t : o.tasks) {
					Task task = taskmap.get(t.id) != null ? taskmap.get(t.id) : new RTTask(t, domain);
					task.updateTimeoutIfNecessary(timeout);
					DataFlow flow = new DataFlow(div, task, null);
					flow.takeEffect();
				}
				end = div;
			}
			else {
				IUserTask t = o.tasks.get(0);
				Task task = taskmap.get(t.id) != null ? taskmap.get(t.id) : new RTTask(t, domain);
				task.updateTimeoutIfNecessary(timeout);
				end = task;
			}
			DataFlow flow = new DataFlow(start, end, condition);
			flow.takeEffect();
			flows.add(flow);
		}
		if (unit.independentTasks.size() > 0) {
			FlowObj conv = new ConvergeGateway(false);
			for (IUserTask t : unit.independentTasks) {
				Task task = new RTTask(t, domain);
				task.updateTimeoutIfNecessary(timeout);
				DataFlow flow = new DataFlow(task, conv, ConditionExp.EXP_CODE_OK);
				flow.takeEffect();
				flows.add(flow);
			}
		}
		addHeadAndTail(flows, job);
		return job;
		
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private static void addHeadAndTail(List<DataFlow> flows, JobFlow job) {
		List<FlowObj> ends = new LinkedList<FlowObj>();
		for (DataFlow flow : flows) {
			flow.getSource().setJobFlow(job);
			flow.getTarget().setJobFlow(job);
			if (flow.getSource().getInFlows().size() == 0)
				job.addHeadFlowObj(flow.getSource());
			if (flow.getTarget().getOutFlows().size() == 0)
				ends.add(flow.getTarget());
		}
		if (ends.size() == 0)
			throw new ZslsRuntimeException("Invalid unit for no end point");
		if (ends.size() == 1) {
			FlowObj obj = ends.get(0);
			ConditionExp condition = obj instanceof Task ? ConditionExp.EXP_CODE_OK : null;
			job.addTailFlowObj(obj, condition);
		} else {
			FlowObj conv = new ConvergeGateway(false);
			conv.setJobFlow(job);
			for (FlowObj obj : ends) {
				ConditionExp condition = obj instanceof Task ? ConditionExp.EXP_CODE_OK : null;
				DataFlow flow = new DataFlow(obj, conv, condition);
				flow.takeEffect();
			}
			job.addTailFlowObj(conv, null);
		}
	}
	
	public static TaskInfo getTaskInfo(Task task) {
		TaskInfo info = new TaskInfo();
		info.setTaskId(new TaskId(task.getTaskId()));
		info.setData(task.getParams());
		Date date = task.getJobFlow().getGenerateTime();
		if (date != null)
			info.setGenerateTime(date.getTime());
		IRetryStrategy retry = task.getRetryOp();
		if (retry != null) {
			info.getData().put(ZslsConstants.TASKP_RETRY_CONDITION, retry.condition);
			info.getData().put(ZslsConstants.TASKP_RETRY_NUM, String.valueOf(retry.num));
		}
		return info;
	}
	
}
