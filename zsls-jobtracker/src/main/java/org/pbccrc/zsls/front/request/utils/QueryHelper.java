package org.pbccrc.zsls.front.request.utils;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.domain.DomainInfo;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.exception.SchedException;
import org.pbccrc.zsls.front.result.Result;
import org.pbccrc.zsls.front.result.ResultLeaf;
import org.pbccrc.zsls.front.result.ResultLeafList;
import org.pbccrc.zsls.front.result.ResultNode;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.JobManager;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.nodes.WorkNode;
import org.pbccrc.zsls.tasks.LocalJobManager;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.utils.DateUtils;
import org.pbccrc.zsls.utils.JsonSerilizer;

public class QueryHelper {
	
	private AppContext context;
	public QueryHelper(AppContext context) {
		this.context = context;
	}
	
	public Result queryRunningUnits(String domain) throws SchedException {
		ResultNode ret = new ResultNode();
		ResultLeafList result = new ResultLeafList();
		ret.addChild(ResultNode.UNITS, result);
		DomainType dtype = context.getDomainManager().getDomainType(domain);
		JobManager manager = LocalJobManager.getJobManager(domain, dtype);
		if (manager != null) {
			List<String> units = manager.getRegisteredUnits();
			for (String id : units)
				result.addElement(id);
		}
		return ret;
	}
	
	public Result queryRunningTask() throws SchedException {
		ResultNode result = new ResultNode();
		try {
			for (String domain : context.getDomainManager().getRTDomains()) {
				ResultNode domainR = new ResultNode();
				result.addChild(domain, domainR);
				if (context.getNodeManager().getNodes(domain) == null) 
					continue;
				Collection<WorkNode> nodes = context.getNodeManager().getNodes(domain).values();
				for (WorkNode node : nodes) {
					ResultLeafList nodeR = new ResultLeafList();
					domainR.addChild(node.getNodeId().toString(), nodeR);
					for (TaskId task : node.getRunningTasks()) {
						nodeR.addElement(task.id);
					}
				}
			}
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}
	
	public Result queryRunningTaskDT() throws SchedException {
		ResultNode result = new ResultNode();
		try {
			for (Map.Entry<String, DomainInfo> d: context.getDomainManager().getDTDomainInfos().entrySet()) {
				ResultNode domainR = new ResultNode();
				result.addChild(d.getKey(), domainR);
				if (context.getNodeManager().getNodes(d.getKey()) == null)
					continue;
				Collection<WorkNode> nodes = context.getNodeManager().getNodes(d.getKey()).values();
				for (WorkNode node : nodes) {
					ResultLeafList nodeR = new ResultLeafList();
					domainR.addChild(node.getNodeId().toString(), nodeR);
					for (TaskId task : node.getRunningTasks()) {
						nodeR.addElement(task.id);
					}
				}
			}
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}
	
	public Result queryUnits() throws SchedException {
		try {
			List<ServerQuartzJob> sList = context.getJobStore().fetchQuartzJobs();
			ResultNode result = new ResultNode();
			for (ServerQuartzJob job : sList) {
				ResultNode unitR = new ResultNode();
				unitR.setStatus(job.getJobStat().toString());
				if (job.getLastExecuteTime() != null)
					unitR.setLastExeTime(DateUtils.format(job.getLastExecuteTime()));
				unitR.setExpression(job.getExpression());
				result.addChild(job.getJobId(), unitR);
			}
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}
	
	public Result queryUnitByDate(String domain, String time, int start, int end) throws SchedException {
		Date date = null;
		try {
			date = time == null ? null : DateUtils.getDate(time);
			List<RTJobFlow> sList = context.getJobStore().fetchUnitsByDate(domain, date, start, end);
			long recordsNum = context.getJobStore().fetchJobsNum(domain, date);
			ResultNode result = new ResultNode();
			ResultNode units = new ResultNode();
			for (RTJobFlow u : sList) {
				ResultNode unitR = new ResultNode();
				units.addChild(u.getJobId().toString(), unitR);
				String unitStat = u.isFinished() ? "FINISH" : "INITIAL";
				unitR.setStatus(unitStat);
				unitR.setCreatetime(u.getGenerateTime().toString());
				unitR.setPreUnit(u.getPreUnit() == null ? null : u.getPreUnit().toString());
				unitR.setDomain(domain);
			}
			result.setUnits(units);
			result.setRecordsNum(recordsNum);
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}
	
	public Result queryTaskByUnit(String unitId) throws SchedException {
		try {
			ResultNode result = new ResultNode();
			ServerQuartzJob job = context.getJobStore().fetchQuartzJob(unitId);
			JobFlow jobFlow = job.generateJobInstance();
			if (job.getLastExecuteTime() != null)
				context.getJobStore().updateTaskStatsForJob(jobFlow, job.getLastExecuteTime());
			//JobFlow jobFlow = LocalJobManager.getDTJobManager().getUnit(unitId);
			if (jobFlow != null) {
				Iterator<Task> it = jobFlow.getTaskIterator();
				while (it.hasNext()) {
					Task task = it.next();
					ResultNode rNode = new ResultNode();
					rNode.setDomain(task.getDomain());
					rNode.setStatus(task.getStatus().name());
					if (task.getResultMsg() != null) {
						rNode.setResultMsg(task.getResultMsg().keymessage);
						rNode.setFeedbackMsg(task.getResultMsg().feedback);
					}
					result.addChild(task.getTaskId(), rNode);
				}
			}
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}
	
	public Result queryTaskByUnit(String unitId, String domain) throws SchedException {
		try {
			RTJobFlow unit = context.getJobStore().fetchJob(domain, RTJobId.fromString(unitId));
			ResultNode result = new ResultNode();
			ResultLeafList list = new ResultLeafList();
			result.addChild(unitId, list);
			
			Iterator<Task> it = unit.getTaskIterator();
			while (it.hasNext()) {
				Task t = it.next();
				ResultNode task = new ResultNode();
				task.addParam(ResultNode.TASK_ID, t.getTaskId());
				TaskStat stat = t.getStatus();
				Task memTask = LocalJobManager.getRTJobManager(domain).getTask(t.getTaskId());
				if (memTask != null)
					stat = memTask.getStatus();
				task.addParam(ResultNode.STAT, stat);
				task.addParam(ResultNode.PARAMETERS, t.getParams()== null ? "" : JsonSerilizer.serilize(t.getParams()));
				task.addParam(ResultNode.RESULT_MESSAGE, t.getResultMsg() == null ? "" : t.getResultMsg().keymessage);
				task.addParam(ResultNode.FEEDBACK_MESSAGE, t.getResultMsg() == null ? "" : t.getResultMsg().feedback);
				list.addElement(task.data());
			}
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}
	
	public Result queryDomainDT() throws SchedException {
		try {
			ResultNode result = new ResultNode();
			Map<String, DomainInfo> infos = context.getDomainManager().getDTDomainInfos();
			Iterator<Map.Entry<String, DomainInfo>> it = infos.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, DomainInfo> entry = it.next();
				String domain = entry.getKey();
				DomainInfo info = entry.getValue();
				result.addParam(domain, info.status);
			}
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}
	
	public Result queryDomain() throws SchedException {
		try {
			ResultNode result = new ResultNode();
			Map<String, DomainInfo> infos = context.getDomainManager().getRTDomainInfos();
			Iterator<Map.Entry<String, DomainInfo>> it = infos.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, DomainInfo> entry = it.next();
				String domain = entry.getKey();
				DomainInfo info = entry.getValue();
				result.addParam(domain, info.status);
			}
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}

	public Result queryUnitBySwift(String swiftId, String domain) throws SchedException {
		try {
			RTJobFlow unit = context.getJobStore().fetchJobBySwiftNum(domain, swiftId);
			ResultLeaf result = new ResultLeaf();
			if (unit != null) {
				result.setData(unit.getJobId().toString());
			}
			return result;
		} catch (Exception e) {
			throw new SchedException(e);
		}
	}
}
