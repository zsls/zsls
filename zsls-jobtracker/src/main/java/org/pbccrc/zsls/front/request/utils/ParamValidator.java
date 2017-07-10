package org.pbccrc.zsls.front.request.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.pbccrc.zsls.api.client.IConvergeGateway;
import org.pbccrc.zsls.api.client.IDataFlow;
import org.pbccrc.zsls.api.client.IDivergeGateway;
import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.ITask;
import org.pbccrc.zsls.api.client.old.IRelation;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.old.IUserTask;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.front.request.QRequest;
import org.pbccrc.zsls.front.request.UserRequest;
import org.pbccrc.zsls.front.request.UserRequest.JobType;
import org.pbccrc.zsls.front.request.UserRequest.QueryType;
import org.pbccrc.zsls.front.request.UserRequest.SubType;
import org.pbccrc.zsls.tasks.dt.QuartzTaskManager;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.utils.DateUtils;
import org.pbccrc.zsls.utils.TaskUtil;

public class ParamValidator {
	
	private static boolean validateDomain(String domain, AppContext context) {
		return domain != null && context.getDomainManager().containsDomain(domain);
	}
	private static boolean validateCmdQuery(String query) {
		return UserRequest.PARAM_VAL_PAUSE.equals(query) ||
					UserRequest.PARAM_VAL_START.equals(query) ||
					UserRequest.PARAM_VAL_STOP.equals(query) ||
					UserRequest.PARAM_VAL_ADDDM.equals(query) ||
					UserRequest.PARAM_VAL_DELDM.equals(query);
	}
	private static boolean validateQuartzCmdQuery(String query) {
		return UserRequest.PARAM_VAL_CANCEL.equals(query) ||
				UserRequest.PARAM_VAL_RESUME.equals(query);
	}
	
	private static void markInvalid(ValidResult ret, String message) {
		ret.valid = false;
		ret.info = message;
	}
	
	public static ValidResult validateRequest(QRequest request, AppContext context) {
		ValidResult ret = new ValidResult();
		UserRequest userRequest = request.getUserRequest();
		QueryType type = userRequest.getQueryType();
		String domain = userRequest.getDomain();
		String query = userRequest.getQuery();
		JobType jobType = userRequest.getJobType();
		SubType subType = userRequest.getSubType();
		
		if (jobType == JobType.Null) {
			markInvalid(ret, "invalid jobType");
			return ret;
		}
		
		if (type == QueryType.SchedStatQuery) {
			SubType subtype = userRequest.getSubType();
			if (subtype != SubType.Domain && subtype != SubType.Running && jobType != JobType.DT && !validateDomain(domain, context)) {
				markInvalid(ret, "invalid domain");
				return ret;
			}
			if (jobType == JobType.DT && (subType != SubType.Unit && subType != SubType.Task && subType != SubType.Running && subType != SubType.Domain)) {
				markInvalid(ret, "invalid DT schedulestatquery");
				return ret;
			}
		}
		
		if (type == QueryType.Null 
				|| (type == QueryType.ScheduleRequest && jobType != JobType.RT)
				|| ((type == QueryType.CronQuartzJob || type == QueryType.SimpleQuartzJob) && jobType != JobType.DT)) {
			markInvalid(ret, "invalid type");
		}
		else if (type == QueryType.ScheduleRequest) {
			if (!validateDomain(domain, context))
				markInvalid(ret, "invalid domain");
			if (!checkValid(userRequest.getScheduleUnit()))
				markInvalid(ret, "invalid ScheduleUnit");
		}
		else if (type == QueryType.SchedCMDQuery && !validateCmdQuery(query)) {
			markInvalid(ret, "invalid query");
		}
		else if (type == QueryType.SimpleQuartzJob || type == QueryType.CronQuartzJob) {
			/*List<String> rpTasks = hasRepeatTasks(userRequest.getJobFlow());
			if (rpTasks != null) {
				StringBuilder ts = new StringBuilder("repeat tasks:");
				for(String t : rpTasks)
					ts.append(" [" + t + "] ");
				markInvalid(ret, ts.toString());
			}
			if (rpTasks == null && !checkValid(userRequest.getJobFlow()))
				markInvalid(ret, "invalid jobLow");*/
		}
		else if (type == QueryType.QuartzCmd) {
			if (query == null || query.trim().isEmpty() || !validateQuartzCmdQuery(userRequest.getCmd()))
				markInvalid(ret, "invalid cmd or query");
		}
		else if (type == QueryType.SchedStatQuery) {
			SubType subtype = userRequest.getSubType();
			if (subtype == SubType.Null) {
				markInvalid(ret, "invalid subtype");
			}
			else if (subtype == SubType.Task) {
				String time = userRequest.getTime();
				if (time != null && !DateUtils.checkDateValid(time))
					markInvalid(ret, "invalid time");
				if (userRequest.getStart() >= userRequest.getEnd()) {
					userRequest.setDefaultStart();
					userRequest.setDefaultEnd();
				}
			}
			else if (subtype == SubType.Unit && (userRequest.getUnitId() == null
					|| (jobType == JobType.RT && RTJobId.fromString(userRequest.getUnitId()) == null))) {
				markInvalid(ret, "invalid unitid");
			}
			else if (subtype == SubType.Swift && query == null) {
				markInvalid(ret, "invalid query");
			}
		}
		else if (type == QueryType.SchedRedoTask || type == QueryType.DisableNodeQuery) {
			if (query == null)
				markInvalid(ret, "invalid query");
		}
		return ret;
	}
	
	public static List<String> hasRepeatTasks(IJobFlow jobFlow) {
		if (jobFlow == null)
			return null;
		List<String> rpTasks = null;
		Set<String> tasks = new HashSet<String>();
		ArrayList<ServerQuartzJob> jobs = QuartzTaskManager.getInstance().getJobs();
		for (ServerQuartzJob job : jobs) {
			IJobFlow flow = job.getOrigJob();
			Iterator<Object> f = flow.flowObjs.iterator();
			while (f.hasNext()) {
				Object o = f.next();
				if (o instanceof ITask)
					tasks.add(((ITask)o).id);
			}
		}
		Iterator<Object> it = jobFlow.flowObjs.iterator();
		while (it.hasNext()) {
			Object o = it.next();
			if (o instanceof ITask) {
				ITask t = (ITask)o;
				if (tasks.contains(t.id)) {
					if (rpTasks == null)
						rpTasks = new ArrayList<String>();
					rpTasks.add(t.id);
				}
			}
		}
		return rpTasks;
	}
	
	public static boolean checkValid(IScheduleUnit unit) {
		if (unit == null) 
			return false;
		if (unit.independentTasks.size() == 0 && unit.relations.size() == 0)
			return false;
		for (IUserTask task : unit.independentTasks) {
			if (task.id == null)
				return false;
		}
		for (IRelation relation : unit.relations) {
			if (relation.preTasks.id == null || relation.postTasks.id == null)
				return false;
			if (relation.preTasks.tasks.size() == 0 || relation.postTasks.tasks.size() == 0)
				return false;
			for (IUserTask task : relation.preTasks.tasks) {
				if (task.id == null) return false;
			}
			for (IUserTask task : relation.postTasks.tasks) {
				if (task.id == null) return false;
			}
		}
		return true;
	}
	
	public static boolean checkValid(IJobFlow jobFlow) {
		if (jobFlow == null)
			return false;
		int countGateway = 0;
		Map<String, Boolean> visitedMap = new HashMap<String, Boolean>();
		Map<String, Object> typeMap = new HashMap<String, Object>();
		Iterator<Object> it = jobFlow.flowObjs.iterator();
		
		while (it.hasNext()) {
			Object o = it.next();
			if (o instanceof IDivergeGateway) {
				countGateway ++;
				if (visitedMap.containsKey(((IDivergeGateway)o).id))
					return false;
				visitedMap.put(((IDivergeGateway)o).id, false);
				typeMap.put(((IDivergeGateway)o).id, o);
			}
			else if (o instanceof IConvergeGateway) {
				countGateway --;
				if (visitedMap.containsKey(((IConvergeGateway)o).id))
					return false;
				visitedMap.put(((IConvergeGateway)o).id, false);
				typeMap.put(((IConvergeGateway)o).id, o);
			}
			else if (o instanceof ITask) {
				if (visitedMap.containsKey(((ITask)o).id))
						return false;
				for (String k :((ITask)o).params.keySet()) {
					if (k == null || k.contains(" "))
						return false;
				}
				if (((ITask)o).partitions < 0)
					return false;
				visitedMap.put(((ITask)o).id, false);
				typeMap.put(((ITask)o).id, o);
			}
		}
		if (countGateway != 0)
			return false;
		
		Iterator<IDataFlow> iter = jobFlow.dataFlows.iterator();
		Map<String, List<String>> srcMap = new HashMap<String, List<String>>();
		Map<String, List<String>> tarMap = new HashMap<String, List<String>>();
		while (iter.hasNext()) {
			IDataFlow dFlow = iter.next();
			if (TaskUtil.JOB_START.equalsIgnoreCase(dFlow.source))
				dFlow.source = TaskUtil.JOB_START;
			if (TaskUtil.JOB_END.equalsIgnoreCase(dFlow.target))
				dFlow.target = TaskUtil.JOB_END;
			
			if (!visitedMap.containsKey(dFlow.source) && !TaskUtil.JOB_START.equals(dFlow.source))
				return false;
			
			if (srcMap.get(dFlow.source) == null)
				srcMap.put(dFlow.source, new ArrayList<String>());
			srcMap.get(dFlow.source).add(dFlow.target);
			
			if (!visitedMap.containsKey(dFlow.target) && !TaskUtil.JOB_END.equals(dFlow.target))
				return false;
			
			if (tarMap.get(dFlow.target) == null)
				tarMap.put(dFlow.target, new ArrayList<String>());
			tarMap.get(dFlow.target).add(dFlow.source);
		}
		
		if (srcMap.get(TaskUtil.JOB_START) == null || tarMap.get(TaskUtil.JOB_END) == null)
			return false;
		if (!isLegalJob(srcMap, tarMap, visitedMap, typeMap))
			return false;
		return true;
	}
	
	private static boolean isLegalJob(Map<String, List<String>> srcMap, Map<String, List<String>> tarMap,
			Map<String, Boolean> visitedMap, Map<String, Object> typeMap) {
		Stack<String> branch = new Stack<String> ();
		branch.push(TaskUtil.JOB_START);
		
		while (!branch.isEmpty()) {
			String curNode = branch.peek();
			
			while (!TaskUtil.JOB_END.equals(curNode)) {
				List<String> tarList = srcMap.get(curNode);
				if (tarList == null || tarList.isEmpty())
					return false;
				curNode = branch.push(tarList.get(0));
			}
			
			if (!isLegalBranch(branch.subList(0, branch.size()), typeMap))
				return false;
			if (!reviseBranches(branch, typeMap, srcMap, tarMap))
				return false;
			while (!branch.isEmpty() && (curNode = branch.pop()) != null) {
				if (typeMap.get(curNode) instanceof IDivergeGateway) {
					if (!srcMap.get(curNode).isEmpty()) {
						branch.push(curNode);
						break;
					}
				}	
			}
		}
		return true;
	}
	
	private static boolean reviseBranches(List<String> branch, Map<String, Object> typeMap, 
			Map<String, List<String>> srcMap, Map<String, List<String>> tarMap) {
		int i  = 0, lastDivIndex = -1, lastConvIndex = -1;
		for (; i < branch.size(); i++) {
			if (!TaskUtil.JOB_END.equals(branch.get(i)) && srcMap.get(branch.get(i)).size() > 1) {
				lastDivIndex = i;
				if (!(typeMap.get(branch.get(lastDivIndex)) instanceof IDivergeGateway))
					return false;
			}
		}
		
		for (i = lastDivIndex + 1; i < branch.size(); i++) {
			if (!TaskUtil.JOB_START.equals(branch.get(i)) && tarMap.get(branch.get(i)).size() > 1) {
				lastConvIndex = i;
				if (!(typeMap.get(branch.get(lastConvIndex)) instanceof IConvergeGateway))
					return false; 
				break;
			}
		}
		
		if ((lastDivIndex > 0 && lastConvIndex < 0) || (lastDivIndex < 0 && lastConvIndex > 0))
			return false;
		
		if (lastDivIndex > 0 && lastConvIndex < branch.size() - 1) {
			
			tarMap.get(branch.get(lastDivIndex + 1)).remove(branch.get(lastDivIndex));
			srcMap.get(branch.get(lastDivIndex)).remove(branch.get(lastDivIndex + 1));
			
			tarMap.get(branch.get(lastConvIndex)).remove(branch.get(lastConvIndex - 1));
			srcMap.get(branch.get(lastConvIndex - 1)).remove(branch.get(lastConvIndex));
			// trim the useless branch nodes
			for (int cur = lastDivIndex + 1; cur < lastConvIndex; cur ++) {
				srcMap.get(branch.get(cur)).clear();
				tarMap.get(branch.get(cur)).clear();
			}
		}
		else
			for (String node : branch) {
				if (!TaskUtil.JOB_END.equals(node))
					srcMap.get(node).clear();
				if (!TaskUtil.JOB_START.equals(node))
					tarMap.get(node).clear();
			}
		return true;
	}
	
	private static boolean isLegalBranch(List<String> branch, Map<String, Object> typeMap) {
		int gatewayCount = 0;
		if (!TaskUtil.JOB_START.equals(branch.get(0)) 
			|| !TaskUtil.JOB_END.equals(branch.get(branch.size() - 1)))
			return false;
		for (String node : branch) {
			Object nodeType = typeMap.get(node);
			if (nodeType instanceof IDivergeGateway) {
				gatewayCount ++;
			}
			if (nodeType instanceof IConvergeGateway)
				gatewayCount --;
			if (gatewayCount < 0)
				return false;
		}
		if (gatewayCount != 0)
			return false;
		return true;
	}

}
