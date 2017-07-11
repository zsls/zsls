package org.pbccrc.zsls.jobstore;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.Task.ExecuteResult;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobFlow.RJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobId;

public interface JobStore {
	
	/*********------------------ 实时任务 ---------------------********/
	
	/* 初始化域 */
	boolean initForDomain(String domain);
	
	/* 清理域 */
	boolean cleanDomain(String domain);
	
	/* 存储作业 */
	long instoreJob(RTJobFlow unit, IScheduleUnit origUnit);
	
	/* 更新作业状态 */
	boolean updateJob(String domain, RTJobId id, RJobStat stat);
	
	/* 更新任务状态 */
	boolean updateTask(String domain, TaskId task, TaskStat stat, ExecuteResult result);
	
	/* 作业是否完成 */
	boolean isJobFinish(String domain, RTJobId id);
	
	/* 读取根据作业ID读取作业 */
	RTJobFlow fetchJob(String domain, RTJobId id);
	
	/* 根据业务号读取作业 */
	RTJobFlow fetchJobBySwiftNum(String domain, String swiftId);
	
	/* 
	 * 从第unitid为id的单元开始连续读取若干个未完成的作业，
	 * 并且保证读取的作业中所有任务的集合数不超过taskLimit 
	 * 
	 */
	boolean fetchJobs(String domain, RTJobId id, List<RTJobFlow> result, int taskLimit);
	
	/* 根据日期读取作业 */
	List<RTJobFlow> fetchUnitsByDate(String domain, Date date, int start, int end);
	
	/* 获取第一个未完成的作业ID */
	RTJobId getFirstUnfinishedUnit(String domain);
	
	/* 获取最后一个作业的id */
	RTJobId getLastUnit(String domain);
	/* 获取指定域的作业数量 */
	long fetchJobsNum(String domain, Date date);
	
	
	/*********------------------- 定时任务 --------------------********/
	
	boolean storeQuartzJob(ServerQuartzJob job, QuartzTrigger trigger, String jobFlow);
	
	boolean deleteQuartzJob(String id);
	
	boolean addJobFlowInstance(ServerQuartzJob job, Date date);
	
	boolean updateTaskStatsForJob(JobFlow job, Date date);
	
	boolean updateJobStatus(String jobId, QJobStat stat);
	
	boolean updateTaskResult(String jobId, String taskId, Date date, TaskResult ret);
	
	List<ServerQuartzJob> fetchQuartzJobs();
	
	ServerQuartzJob fetchQuartzJob(String jobId);
	
	Map<String, String> fetchRuntimeParams(String jobId);

}
