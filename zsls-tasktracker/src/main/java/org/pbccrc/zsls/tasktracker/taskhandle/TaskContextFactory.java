package org.pbccrc.zsls.tasktracker.taskhandle;

import java.util.Map;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.api.thrift.records.TTaskInfo;
import org.pbccrc.zsls.api.thrift.records.TaskType;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.tasktracker.ClientContext;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskContext.RetryStrategy;

public class TaskContextFactory {
	private static Logger L = Logger.getLogger(TaskContextFactory.class.getSimpleName());
	
	private ClientContext context;
	public TaskContextFactory(ClientContext context) {
		this.context = context;
	}
	
	public TaskContext newTaskContext(TaskType type, TTaskInfo taskInfo, boolean retry) {
		TaskDetail detail = new TaskDetail(type, taskInfo);
		detail.setResubmit(retry);
		TaskContextImpl context = new TaskContextImpl(detail);
		context.setTaskDetail(detail);
		String retryCondition = taskInfo.data.get(ZslsConstants.TASKP_RETRY_CONDITION);
		String retryNum = taskInfo.data.get(ZslsConstants.TASKP_RETRY_NUM);
		if (retryCondition != null && retryNum != null) {
			RetryStrategy retryOp = null;
			try {
				int num = Integer.parseInt(retryNum);
				retryOp = new RetryStrategy(retryCondition, num);	
			} catch (Exception e) {
				L.error("ignore invalid retry condition, error: " + e);
			}
			if (retryOp != null)
				context.setRetryStrategy(retryOp);
		}
		return context;
	}
	
	public void updateContextForRetry(TaskContext context) {
		int retry = context.getLocalRetryTime();
		((TaskContextImpl)context).setLocalRetryTime(++retry);
	}
	
	private class TaskContextImpl implements TaskContext {
		
		private TaskDetail taskInfo;
		private long startTime;
		private long endTime;
		private RetryStrategy retryOp;
		private int localRetryTime;
		
		private TaskContextImpl(TaskDetail taskInfo) {
			this.taskInfo = taskInfo;
		}
		
		@Override
		public TaskDetail getTaskDetail() {
			return taskInfo;
		}
		public void setTaskDetail(TaskDetail info) {
			this.taskInfo = info;
		}
		@Override
		public long getStartTimestamp() {
			return startTime;
		}
		public long getEndTimestamp() {
			return endTime;
		}
		@Override
		public int getLocalRetryTime() {
			return localRetryTime;
		}
		public void setLocalRetryTime(int retryNum) {
			this.localRetryTime = retryNum;
		}
		@Override
		public RetryStrategy getRetryStrategy() {
			return retryOp;
		}
		public void setRetryStrategy(RetryStrategy retryOp) {
			this.retryOp = retryOp;
		}
		@Override
		public ResultWriter getResultWriter() {
			return new ResultWriterImpl(taskInfo.getTaskId());
		}
		
	}	
	
	private class ResultWriterImpl implements ResultWriter {
		private String taskId;
		private ResultWriterImpl(String taskId) {
			this.taskId = taskId;
		}
		@Override
		public void writeFeedbackMessage(String msg) {
			TaskExecutionInfo info = context.getTaskManager().getTaskExecutionInfo(taskId);
			if (info != null)
				info.feedbackMsg = msg;
		}
		@Override
		public void writeKeyMessage(String msg) {
			TaskExecutionInfo info = context.getTaskManager().getTaskExecutionInfo(taskId);
			if (info != null)
				info.keyMessage = msg;
		}
		@Override
		public void updateRuntimeParams(Map<String, String> data) {
			TaskExecutionInfo info = context.getTaskManager().getTaskExecutionInfo(taskId);
			if (info != null)
				info.runtimeParams = data;
		}
	}

}
