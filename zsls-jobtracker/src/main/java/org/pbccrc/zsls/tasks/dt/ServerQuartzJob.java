package org.pbccrc.zsls.tasks.dt;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.state.ZslsStateMachine;
import org.pbccrc.zsls.state.ZslsStateMachineFactory;
import org.pbccrc.zsls.utils.DateUtils;
import org.pbccrc.zsls.utils.TaskUtil;
import org.quartz.Trigger;

public class ServerQuartzJob {
	public static final int TRIGGER_LVL_NORMAL = 0;	//前一个周期结束
	public static final int TRIGGER_LVL_STUCK  = 1;	//前一个周期阻塞
	public static final int TRIGGER_LVL_FORCE  = 2;	//若前一个周期作业未结束，直接杀掉
	
	private String jobId;

	private Trigger trigger;
	
	private IJobFlow origJob;
	
	private Date lastExecuteTime;
	
	private String expression;
	
	private int triggerCondition;
	
	private Map<String, Map<String, String>> tasksMeta;
	
	private static final ZslsStateMachineFactory<QJobStat> stateFactory =
			new ZslsStateMachineFactory<QJobStat>(QJobStat.Init)
			.addTransition(QJobStat.Init, QJobStat.Init)
			.addTransition(QJobStat.Init, QJobStat.Cancel)
			.addTransition(QJobStat.Init, QJobStat.Run)
			.addTransition(QJobStat.Init, QJobStat.Finish)
			.addTransition(QJobStat.Init, QJobStat.Stuck)
			
			.addTransition(QJobStat.Run, QJobStat.Finish)
			.addTransition(QJobStat.Run, QJobStat.Cancel)
			.addTransition(QJobStat.Run, QJobStat.Stuck)
			
			.addTransition(QJobStat.Finish, QJobStat.Run)
			.addTransition(QJobStat.Finish, QJobStat.Cancel)
			
			.addTransition(QJobStat.Stuck, QJobStat.Finish)
			.addTransition(QJobStat.Stuck, QJobStat.Cancel)
			
			.addTransition(QJobStat.Cancel, QJobStat.Init)
			.build();
	
	private ZslsStateMachine<QJobStat> jobStat = stateFactory.makeStateMachine();
	
	public ServerQuartzJob(QuartzTrigger trigger, IJobFlow jobFlow) {
		this.origJob = jobFlow;
		this.jobId = jobFlow.id;
		this.trigger = trigger.getTrigger(jobId);
		this.tasksMeta = new HashMap<String, Map<String, String>>();
		this.triggerCondition = jobFlow.refreshIfPrevJobStucked ? TRIGGER_LVL_STUCK : TRIGGER_LVL_NORMAL;
		if (jobFlow == null || trigger == null)
			throw new IllegalArgumentException("null trigger or jobFlow");
	}
	
	public JobFlow generateJobInstance() {
		try {
			JobFlow job = TaskUtil.parseJobFlow(origJob);
			return job;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static enum QJobStat {
		Init(0),
		Run(1),
		Stuck(2),
		Finish(3),
		Cancel(4);
		int value;
		QJobStat(int value) {
			this.value = value;
		}
		public int getVal() {
			return value;
		}
		public static QJobStat getJobStat(int val) {
			for (QJobStat stat : QJobStat.values()) {
				if (stat.value == val)
					return stat;
			}
			return QJobStat.Init;
		}
	}
	
	public QJobStat getJobStat() {
		return jobStat.getCurrentState();
	}
	
	public QJobStat changeStatus(QJobStat stat) {
		return jobStat.doTransition(stat);
	}
	
	public Date getLastExecuteTime() {
		return lastExecuteTime;
	}
	
	public void setLastExecuteTime(Date date) {
		lastExecuteTime = date;
	}

	public String getJobId() {
		return jobId;
	}
	
	public Trigger getTrigger() {
		return trigger;
	}
	
	public void setExpression(String expression) {
		this.expression = expression;
	}
	
	public String getExpression() {
		return expression;
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("(");
		b.append("jobId: ").append(jobId).append(", ");
		b.append("type: ").append(trigger.getClass().getSimpleName()).append(", ");
		b.append("status: ").append(jobStat.getCurrentState()).append(", ");
		String time = lastExecuteTime == null ? null : DateUtils.format(lastExecuteTime);
		b.append("lastExecuteTime: ").append(time);
		b.append(")");
		return b.toString();
	}
	
	public void updateRuntimeParams(String taskId, Map<String, String> meta) {
		if (meta != null)
			this.tasksMeta.put(taskId, meta);
	}
	
	public Map<String, String> getRuntimeParams(String taskId) {
		return this.tasksMeta.get(taskId);
	}
	
	public int getTriggerCondition() {
		return this.triggerCondition;
	}
	
	public IJobFlow getOrigJob() {
		return this.origJob;
	}
}
