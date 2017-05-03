package org.pbccrc.zsls.tasktracker.factory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.pbccrc.zsls.api.thrift.records.HeartBeatRequest;
import org.pbccrc.zsls.api.thrift.records.RegisterRequest;
import org.pbccrc.zsls.api.thrift.records.ReportTaskRequest;
import org.pbccrc.zsls.api.thrift.records.TNodeId;
import org.pbccrc.zsls.api.thrift.records.TTaskId;
import org.pbccrc.zsls.api.thrift.records.TTaskResult;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.tasktracker.ClientContext;
import org.pbccrc.zsls.tasktracker.config.ClientConfig;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskContext;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskExecutionInfo;
import org.pbccrc.zsls.utils.LocalUtils;

public class ClientRecordFactory {
	private ClientContext context;
	
	public ClientRecordFactory(ClientContext context) {
		this.context = context;
	}
	
	public ReportTaskRequest genReportRequest(TaskExecutionInfo info, ClientConfig config) {
		ReportTaskRequest request = new ReportTaskRequest();
		String domain = config.getDomain() == null ? ZslsConstants.DEFAULT_DOMAIN : config.getDomain();
		request.setDomain(domain);
		
		TNodeId id = new TNodeId();
		id.setIp(LocalUtils.getLocalIp());
		id.setPort(config.getRecvPort());
		id.setName(config.getName());
		id.setDomain(domain);
		request.setNodeId(id);
		
		List<TTaskResult> results = new ArrayList<TTaskResult>();
		results.add(genResult(info, config.getDomain()));
		request.setTaskResults(results);
		
		List<TTaskId> tasks = new ArrayList<TTaskId>();
		Collection<TaskExecutionInfo> set = context.getTaskManager().getTaskExecutionInfoSet();
		for (TaskExecutionInfo task : set) {
			String realId = task.context.getTaskDetail().getTaskId();
			TTaskId taskid = new TTaskId(realId);
			tasks.add(taskid);
		}
		request.setRunningTasks(tasks);
		return request;
	}
	private static TTaskResult genResult(TaskExecutionInfo info, String domain) {
		TTaskResult result = new TTaskResult();
		
		if (info.feedbackMsg != null && info.feedbackMsg.length() > 1024) {
			info.feedbackMsg = info.feedbackMsg.substring(0, 1024);
		}
		result.setInfo(info.feedbackMsg);
		result.setKeyMsg(info.keyMessage);
		result.setRuntimeParams(info.runtimeParams);
		TTaskId id = new TTaskId();
		TaskContext context = info.context;
		id.setTaskid(context.getTaskDetail().getTaskId());
		result.setTaskid(id);
		result.setGenerateTime(context.getTaskDetail().getJobTime());
		result.setExecuteTime(context.getEndTimestamp() - context.getStartTimestamp());
		return result;
	}
	
	public RegisterRequest getRegisterRequest() {
		ClientConfig config = context.getConfig();
		RegisterRequest req = new RegisterRequest();
		String domain = config.getDomain() == null ? 
				ZslsConstants.DEFAULT_DOMAIN : config.getDomain();
		req.setDomain(domain);
		boolean isDtDomain = domain.equals(ZslsConstants.DEFAULT_DOMAIN) ?
				true : config.isDtDomain();
		req.setIsDt(isDtDomain);
		req.setMaxnum(config.getMaxTaskNum());
		
		TNodeId id = new TNodeId();
		id.setIp(LocalUtils.getLocalIp());
		id.setPort(config.getRecvPort());
		id.setName(config.getName());
		id.setDomain(domain);
		req.setNodeid(id);
		
		List<TTaskId> tasks = new ArrayList<TTaskId>();
		Collection<TaskExecutionInfo> set = context.getTaskManager().getTaskExecutionInfoSet();
		for (TaskExecutionInfo task : set) {
			String realId = task.context.getTaskDetail().getTaskId();
			TTaskId taskid = new TTaskId(realId);
			tasks.add(taskid);
		}
		req.setRunningTasks(tasks);
		return req;
	}

	public HeartBeatRequest getHeartBeat() {
		HeartBeatRequest req = new HeartBeatRequest();
		ClientConfig config = context.getConfig();
		String domain = config.getDomain() == null ? 
				ZslsConstants.DEFAULT_DOMAIN : config.getDomain();
		
		req.setDomain(domain);
		
		TNodeId id = new TNodeId();
		id.setIp(LocalUtils.getLocalIp());
		id.setPort(config.getRecvPort());
		id.setName(config.getName());
		id.setDomain(domain);
		req.setNodeid(id);
		
		List<TTaskId> tasks = new ArrayList<TTaskId>();
		Collection<TaskExecutionInfo> set = context.getTaskManager().getTaskExecutionInfoSet();
		for (TaskExecutionInfo task : set) {
			String realId = task.context.getTaskDetail().getTaskId();
			TTaskId taskid = new TTaskId(realId);
			tasks.add(taskid);
		}
		req.setRunningTasks(tasks);
		
		return req;
	}

}
