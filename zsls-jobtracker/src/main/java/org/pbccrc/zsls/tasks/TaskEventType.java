package org.pbccrc.zsls.tasks;

public enum TaskEventType {
	
	//任务完成
	COMPLETE,
	
	//任务失败
	FAIL,
	
	//更新节点的正在运行的任务信息
	UPDATE_RUNNING,
	
	
	//RT新任务
	RT_NEW_JOB,
	
	//DT新任务
	DT_NEW_JOB,
	
	//RT一次性触发，比如初始加载完成，状态变为Running，或定时保险触发等。每个RT域一个事件。
	RT_TRIGGER,
	
	//DT一次性触发，比如初始加载完成，状态改变等。所有DT域共享一个事件。
	DT_TRIGGER,
	
	
	//RT任务重做
	REDO_TASK,
	
	//RT新节点加入
	RT_NEW_NODE,
	
	
	//杀掉Job
	KILL_JOB,
	
	//恢复QuartzJob
	RESUME_JOB,

}
