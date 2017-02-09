package org.pbccrc.zsls.jobengine;

import java.util.HashMap;
import java.util.Map;

import org.pbccrc.zsls.api.client.IRetryStrategy;
import org.pbccrc.zsls.jobengine.bpmn.Activity;
import org.pbccrc.zsls.utils.timeout.Expirable;

public class Task extends Activity implements PriorityQueuable, Expirable {
	
	public static enum TaskStat {
		Init(0),
		Finished(1),
		Fail(2),
		Assigned(3),
		ReSubmit(4),
		Timeout(5);
		int val;
		TaskStat(int val) {
			this.val = val;
		}
		public int getVal() {
			return val;
		}
		public static TaskStat getInstance(int val) {
			for (TaskStat stat : TaskStat.values()) {
				if (stat.getVal() == val)
					return stat;
			}
			return null;
		}
	}
	
	// execution result apart from status.
	public static class ExecuteResult {
		public String keymessage;
		public String feedback;
		public ExecuteResult(String msg, String feedback) {
			this.keymessage = msg;
			this.feedback = feedback;
		}
	}
	
	/*////////////////////////////////////////////////////////////////*/
	
	
	public Task(String id, String domain) {
		this.taskId = id;
		this.domain = domain;
	}
	
	
	// domain
	protected String domain;
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	// target node
	protected String targetNode;
	public String getTargetNode() {
		return targetNode;
	}
	public void setTargetNode(String node) {
		this.targetNode = node;
	}
	
	// retry strategy
	protected IRetryStrategy retryOp;
	public IRetryStrategy getRetryOp() {
		return retryOp;
	}
	public void setRetryOp(IRetryStrategy retryOp) {
		this.retryOp = retryOp;
	}


	// stat
	protected volatile TaskStat stat = TaskStat.Init;
	public TaskStat getStatus() {
		return stat;
	}
	public void markAssigned() {
		stat = TaskStat.Assigned;
	}
	public void markStatus(TaskStat stat) {
		this.stat = stat;
	}
	public boolean isFailed() {
		return stat == TaskStat.Fail;
	}
	public boolean canBeAssigned() {
		return stat == TaskStat.Init || stat == TaskStat.ReSubmit;
	}
	
	
	// execute result except status
	protected ExecuteResult resultMsg;
	public ExecuteResult getResultMsg() {
		return resultMsg;
	}
	public void updateExecuteResult(String keymsg, String feedback) {
		resultMsg = new ExecuteResult(keymsg, feedback);
	}
	
	
	// id
	protected String taskId;
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	
	
	// parameters
	protected Map<String, String> params = new HashMap<String, String>();
	public Map<String, String> getParams() {
		return params;
	}
	public void setParams(Map<String, String> params) {
		this.params = params;
	}
	public void addParam(String key, String value) {
		params.put(key, value);
	}

	
	// timeout related 
	protected long timeout = -1;
	public long getTimeout() {
		return timeout;
	}
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}
	public void updateTimeoutIfNecessary(long timeout) {
		if (timeout > 0 && this.timeout <= 0) {
			this.timeout = timeout;
		}
	}
	
	protected long expireTime;
	public Task resetExpireTime() {
		this.expireTime = System.currentTimeMillis() + timeout;
		return this;
	}
	@Override
	public long expireTime() {
		return expireTime;
	}
	@Override
	public boolean timeoutCanceled() {
		return stat == TaskStat.Fail || stat == TaskStat.Finished;
	}
	@Override
	public String getUniqueId() {
		return this.taskId;
	}
	
	
	// priority related
	protected double priority;
	public void setPriority(double value) {
		this.priority = revisePriority(value);
	}	
	protected double revisePriority(double priority) {
		if (priority > PriorityQueuable.MAX_PRIORITY)
			return PriorityQueuable.MAX_PRIORITY;
		if (priority < PriorityQueuable.MIN_PRIORITY)
			return PriorityQueuable.MIN_PRIORITY;
		return priority;
	}
	@Override
	public double getPriority() {
		return priority;
	}
	@Override
	public int compareTo(PriorityQueuable o) {
		if (o == this)
			return 0;
		Task task = (Task)o;
		double val2 = task.getPriority();
		if (priority == val2) {
			String jobId = job.getJobId().toString();
			String targetJobId = task.getJobFlow().getJobId().toString();
			if (jobId.equals(targetJobId))
				return this.taskId.compareTo(task.taskId);
			else
				return jobId.compareTo(targetJobId);
		}
		else {
			return priority > val2 ? -1 : 1;
		}
	}

}
