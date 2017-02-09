package org.pbccrc.zsls.jobstore.zookeeper;

import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTTask;

public class UserTaskChild {

	private String taskId;
	private long unitId;
	private TaskStat taskStat = TaskStat.Init;
	private String feedback;
	
	public UserTaskChild(RTJobFlow JobUnit, RTTask userTask) {
		if (JobUnit == null || userTask == null) {
			throw new IllegalArgumentException("argument is null");
		}
		setTaskId(userTask.getTaskId());
		setUnitId(JobUnit.getJobId().getId());
	}
	public long getUnitId() {
		return unitId;
	}
	private void setUnitId(long unitId) {
		this.unitId = unitId;
	}
	public String getTaskId() {
		return taskId;
	}
	private void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public TaskStat getTaskStat() {
		return taskStat;
	}
	public void setTaskStat(TaskStat taskStat) {
		this.taskStat = taskStat;
	}
	public String getFeedback() {
		return feedback;
	}
	public void setFeedback(String feedback) {
		this.feedback = feedback;
	}
}
