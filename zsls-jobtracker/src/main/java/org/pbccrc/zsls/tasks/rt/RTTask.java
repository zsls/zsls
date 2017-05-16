package org.pbccrc.zsls.tasks.rt;

import java.util.HashMap;

import org.pbccrc.zsls.api.client.IRetryStrategy;
import org.pbccrc.zsls.front.ResultSerializable;
import org.pbccrc.zsls.jobengine.PriorityQueuable;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.utils.StringUtils;

public class RTTask extends Task implements ResultSerializable {
	public static final long DEFAULT_TIMEOUT_MS = 1800 * 1000;	// half an hour by default
	public static final int DEFAULT_SERIALIZE = 0;
	public static final int SERIALIZE_WITHOUT_UNITID = 1;
	
	
	public RTTask(String id, String domain) {
		super(id, domain);
	}
	
	public RTTask(org.pbccrc.zsls.api.client.old.IUserTask task, String domain) {
		super(task.id, domain);
		this.setParams(task.params);
		this.setPriority(revisePriority(task.priority));
		if (task.timeout > 0)
			this.timeout = task.timeout;
		if (task.retryOp != null)
			retryOp = parseRetryStrategy(task.retryOp);
	}
	
	private IRetryStrategy parseRetryStrategy(String retryOp) {
		String[] strs = StringUtils.split(retryOp, ';');
		if (strs.length == 2) {
			try {
				int num = Integer.parseInt(strs[1].trim());
				String condition = strs[0].trim();
				return new IRetryStrategy(condition, num);
			} catch (Exception ignore) {
			}
		}
		return null;
	}
	
	public void updateTaskId(String unitId) {
		this.taskId = unitId + "-" + taskId;
	}
	
	// assign count
	private int assignCount;
	public int getAssignCount() {
		return assignCount;
	}
	public void setAssignCount(int assignCount) {
		this.assignCount = assignCount;
	}
	public int inrementAssignCount() {
		return ++assignCount;
	}

	// stat
	public void markReSubmit() {
		stat = TaskStat.ReSubmit;
	}
	public boolean isReSubmit() {
		return stat == TaskStat.ReSubmit;
	}
	public boolean canRedo() {
		return stat == TaskStat.Finished || stat == TaskStat.Fail;
	}
	

	@Override
	public boolean serialize(StringBuilder builder, int flag) {
		// we serialize to Object directly for now.
		return false;
	}

	@Override
	public Object serialize(int flag) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("taskId", this.taskId);
		if (this.job != null && flag != SERIALIZE_WITHOUT_UNITID)
			map.put("unitId", this.job.getJobId().toString());
		if (this.stat != null) {
			map.put("stat", this.stat.toString());
		}
		return map;
	}

	@Override
	public int compareTo(PriorityQueuable o) {
		if (o == this)
			return 0;
		RTTask task = (RTTask)o;
		double val2 = task.getPriority();
		if (priority == val2) {
			RTJobId id = (RTJobId)task.getJobFlow().getJobId();
			long targetId = id.getId();
			long curId = ((RTJobId)job.getJobId()).getId();
			if (curId == targetId)
				return taskId.compareTo(task.getTaskId());
			else 
				return curId < targetId ? -1 : 1;
		}
		else {
			return priority > val2 ? -1 : 1;
		}
	}
	public boolean equals(Object o) {
		if (o == this)
			return true;
		return false;
	}
	
	public int hashCode() {
		return super.hashCode(); 
	}
	
	public String toString() {
		return this.taskId;
	}
	
}
