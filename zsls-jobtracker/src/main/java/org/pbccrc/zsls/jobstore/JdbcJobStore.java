package org.pbccrc.zsls.jobstore;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.quartz.CronQuartzTrigger;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.api.quartz.SimpleQuartzTrigger;
import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.config.DbConfig;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.exception.store.EntryNotExistException;
import org.pbccrc.zsls.exception.store.JdbcException;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobengine.Task.ExecuteResult;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.jobstore.mysql.Handlers.BatchTaskHandler;
import org.pbccrc.zsls.jobstore.mysql.Handlers.NumberHandler;
import org.pbccrc.zsls.jobstore.mysql.Handlers.QuartzHandler;
import org.pbccrc.zsls.jobstore.mysql.Handlers.QuartzTasksStatHandler;
import org.pbccrc.zsls.jobstore.mysql.Handlers.RuntimeParamHandler;
import org.pbccrc.zsls.jobstore.mysql.Handlers.SimpleTaskInfo;
import org.pbccrc.zsls.jobstore.mysql.Handlers.UnitHandler;
import org.pbccrc.zsls.store.jdbc.JdbcAbstractAccess;
import org.pbccrc.zsls.store.jdbc.Transaction;
import org.pbccrc.zsls.store.jdbc.TransactionFactory;
import org.pbccrc.zsls.store.jdbc.builder.DeleteSql;
import org.pbccrc.zsls.store.jdbc.builder.InsertSql;
import org.pbccrc.zsls.store.jdbc.builder.SelectSql;
import org.pbccrc.zsls.store.jdbc.builder.UpdateSql;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobFlow.RJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.tasks.rt.RTTask;
import org.pbccrc.zsls.utils.DateUtils;
import org.pbccrc.zsls.utils.JsonSerilizer;

import com.google.gson.Gson;

public abstract class JdbcJobStore extends JdbcAbstractAccess implements JobStore {
	
	public static final String TBLNAME_QJOB = "QUARTZ_JOBS";
	public static final String TBLNAME_QTASK = "QUARTZ_TASKS";
	public static final String TBLNAME_QTASK_PARAM = "QUARTZ_TASKPARAM";
	
	public static final String COL_DOMAIN = "DOMAIN";
	public static final String COL_TASK_ID = "TASKID";
	public static final String COL_TASK_UID = "UNITID";
	public static final String COL_TASK_STATUS = "STATUS";
	public static final String COL_TASK_ASSIGNCOUNT = "CNT";
	public static final String COL_TASK_INFO = "INFO";
	
	public static final String COL_UNIT_ID = "ID";
	public static final String COL_UNIT_STATUS = "STATUS";
	public static final long COL_UNIT_PREUNIT_DEF = -1;
	public static final String COL_UNIT_PREUNIT = "PREUNIT";
	public static final String COL_UNIT_SWIFTNUM = "SWIFTNUM";
	public static final String COL_UNIT_CONTENT = "JSONCONTENT";
	public static final String COL_UNIT_DOMAIN = "DOMAIN";
	public static final String COL_UNIT_CTIME = "CREATETIME";
	public static final String COL_UNIT_TASKNUM = "TASKNUM";
	
	public static final String COL_QJ_ID = "ID";
	public static final String COL_QJ_STATUS = "STAT";
	
	/** TRIGGER is keyword in MySql, use TRIGGER_JSON instead
	 * **/
	public static final String COL_QJ_TRIGGER = "TRIGGER_JSON";
	public static final String COL_QJ_EXPRESSION = "EXPRESSION";
	public static final String COL_QJ_JOB = "JOB_FLOW";
	public static final String COL_QJ_TYPE = "TRIGGER_TYPE";
	public static final String COL_QJ_LAST_EXETIME = "LAST_EXETIME"; 
	
	public static final String COL_QT_ID = "TASKID";
	public static final String COL_QT_JID = "JOBID";
	public static final String COL_QT_RESULT = "RESULT";
	public static final String COL_QT_STAT = "STAT";
	public static final String COL_QT_EXETIME = "EXETIME";
	public static final String COL_QT_RUNTIME_PARAM = "PARAM";
	
	private final String SIMPLE_EXPR_NOSTART = "NOSTART";
	private final String SIMPLE_EXPR_NOEND = "NOEND";
	
	
	public JdbcJobStore(DbConfig config) {
		super(config);
	}
	
	private UnitHandler unitHandler = new UnitHandler();
	
	protected NumberHandler numberHandler = new NumberHandler();

	@Override
	public long instoreJob(RTJobFlow unit, IScheduleUnit origUnit) {
		Transaction trans = TransactionFactory.getTransaction();
		long id = -1L;
		try {
			id = unitInstore(unit, origUnit, trans);
			unit.updateUnitIdForAllTasks(id);
			Iterator<Task> it = unit.getTaskIterator();
			while (it.hasNext()) {
				RTTask task = (RTTask)it.next();
				taskInstore(unit, task, trans);
			}
			trans.commit();	
		} catch (Exception e) {
			id = -1L;
			try {
				trans.rollback();
			} catch (Exception ignore) {
			}
			e.printStackTrace();
		} finally {
			try {
				trans.close();
			} catch (Exception ignore) {
			}
		}
		return id;
	}
	
	protected abstract long unitInstore(RTJobFlow unit, IScheduleUnit origUnit, Transaction trans);
	
	private long taskInstore(RTJobFlow unit, RTTask task, Transaction trans) {
		int ret = new InsertSql(getSqlTemplate())
						.inTransition(trans)
						.insert(getTaskTable(unit.getDomain()))
						.columns(COL_TASK_ID, COL_TASK_UID)
						.values(task.getTaskId(), unit.getJobId().getId())
						.doInsert();
		if (ret != 1)
			throw new JdbcException("insert " + task.getTaskId() + " in [" + task.getDomain() + "]");
		return ret;
	}
	
	protected String getUnitTable(String domain) {
		return ZslsConstants.TABLE_NAME_UNIT + "_" + domain;
	}
	
	protected String getTaskTable(String domain) {
		return ZslsConstants.TABLE_NAME_TASK + "_" + domain;
	}
	
	@Override
	public boolean updateTask(String domain, TaskId task, TaskStat stat, ExecuteResult r) {
		int ret = markTask(domain, task, stat, r, null);
		if (ret == 0)
			throw new EntryNotExistException(task.id + " in [" + domain + "]");
		return ret != 0;
	}
	
	private int markTask(String domain, TaskId task, TaskStat stat, ExecuteResult r, Transaction trans) {
		UpdateSql sql = new UpdateSql(getSqlTemplate())
						.inTransition(trans)
						.update()
						.table(getTaskTable(domain))
						.set(COL_TASK_STATUS, stat.getVal());
		if (r != null && r.feedback != null) {
			if (r.feedback.length() > 2048)
				r.feedback = r.feedback.substring(0, 2048);
			String info = JsonSerilizer.serilize(r);
			sql.set(COL_TASK_INFO, info);
		}
		sql.where(COL_TASK_ID + " = ?", task.id);
		int ret = sql.doUpdate();
		return ret;
	}
	
	@Override
	public RTJobFlow fetchJob(String domain, RTJobId id) {
		RTJobFlow sUnit = new SelectSql(getSqlTemplate())
						.select()
						.all()
						.from()
						.table(getUnitTable(domain))
						.where(COL_UNIT_ID + " = ?", id.getId())
						.single(unitHandler);
		if (sUnit == null)
			return null;
		updateTaskStatusForUnit(domain, sUnit);
		return sUnit;
	}
	
	protected void updateTaskStatusForUnit(String domain, RTJobFlow sUnit) {
		Map<String, RTTask> map = fetchTasksByUnit(domain, sUnit.getJobId());
		Iterator<Task> it = sUnit.getTaskIterator();
		while (it.hasNext()) {
			RTTask t = (RTTask)it.next();
			RTTask u = map.get(t.getTaskId());
			if (u == null)
				continue;
			t.markStatus(u.getStatus());
			if (u.getResultMsg() != null) {
				ExecuteResult r = u.getResultMsg();
				t.updateExecuteResult(r.keymessage, r.feedback);
			}
		}
	}
	
	@Override
	public boolean fetchJobs(String domain, RTJobId unitid, List<RTJobFlow> result, int taskLimit) {
		List<RTJobFlow> uList = fetchUnitsWithWindow(domain, unitid, taskLimit);
		// update task status
		int count = 0;
		for (RTJobFlow u: uList) {
			count += u.getTaskNum();
			if (count > taskLimit)
				return false;
			updateTaskStatusForUnit(domain, u);
			result.add(u);
		}
		
		if (count == 0)			return true;
		if (count >= taskLimit)	return false;
		
		// fetch more
		RTJobId lastid = uList.get(uList.size() - 1).getJobId();
		RTJobId newid = new RTJobId(lastid.getId() + 1);
		return fetchJobs(domain, newid, result, taskLimit - count);
	}
	
	protected abstract List<RTJobFlow> fetchUnitsWithWindow(String domain, RTJobId unitid, int windowsize);
	
	
	@Override
	public boolean updateJob(String domain, RTJobId id, RJobStat stat) {
		return updateJob(domain, id, stat.getVal(), null) > 0;
	}

	protected int updateJob(String domain, RTJobId id, int stat, Transaction trans) {
		int ret = new UpdateSql(getSqlTemplate())
						.inTransition(trans)
						.update()
						.table(getUnitTable(domain))
						.set(COL_UNIT_STATUS, stat)
						.where(COL_UNIT_ID + " = ?", id.getId())
						.doUpdate();
		return ret;
	}
	
	@Override
	public abstract List<RTJobFlow> fetchUnitsByDate(String domain, Date date, int start, int end);
	
	protected abstract SelectSql limitResultSet(String sql, int start, int end); 
	
	protected abstract SelectSql sqlSelectWhereDateMatch(SelectSql sql, Date date);
	
	@Override
	public long fetchJobsNum(String domain, Date date) {
		SelectSql sql = new SelectSql(getSqlTemplate())
				.select()
				.columns("COUNT(*)")
				.from()
				.table(getUnitTable(domain));
		sql = date == null ? sql : sqlSelectWhereDateMatch(sql, date);
		Object jobsNum = sql.single();
		return (Long)jobsNum;
	}
	
	@Override
	public boolean isJobFinish(String domain, RTJobId id) {
		RTJobFlow sUnit = fetchJob(domain, id);
		if (sUnit == null)
			throw new EntryNotExistException(id.getId() + " in " + "[" + domain + "]");
		if (sUnit.isFinished())
			return true;
		return false;
	}

	public boolean markTaskReSubmit(String domain, RTJobId unit, TaskId task) {
		Transaction trans = TransactionFactory.getTransaction();
		int ret = markTask(domain, task, TaskStat.ReSubmit, null, trans);
		ret += updateJob(domain, unit, 0, trans);
		try {
			trans.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				trans.close();
			} catch (SQLException ignored) {}
		}
		if (ret != 2)
			return false;
		return true;
	}
	
	@Override
	public RTJobId getFirstUnfinishedUnit(String domain) {
		Long uid = new SelectSql(getSqlTemplate())
						.select()
						.columns(COL_UNIT_ID)
						.from()
						.table(getUnitTable(domain))
						.where(COL_UNIT_STATUS + " != ? ", 1)
						.single(numberHandler);
		if (uid == null)
			return null;
		return new RTJobId(uid.longValue());
	}
	

	protected Map<String, RTTask> fetchTasksByUnit(String domain, RTJobId id) {
		Map<String, RTTask> uList = new SelectSql(getSqlTemplate())
					.select()
					.columns(COL_TASK_ID, COL_TASK_STATUS, COL_TASK_INFO, COL_TASK_ASSIGNCOUNT)
					.from()
					.table(getTaskTable(domain))
					.where(COL_TASK_UID + " = ? ", id.getId())
					.single(new BatchTaskHandler());
		return uList;
	}

	@Override
	public RTJobFlow fetchJobBySwiftNum(String domain, String swiftNum) {
		RTJobFlow unit = new SelectSql(getSqlTemplate())
					.select()
					.all()
					.from()
					.table(getUnitTable(domain))
					.where(COL_UNIT_SWIFTNUM + " = ?",swiftNum)
					.single(unitHandler);
		return unit;
	}
	
	@Override
	public boolean storeQuartzJob(ServerQuartzJob job, QuartzTrigger trigger, String jobFlow) {
		StringBuffer exp = new StringBuffer();
		if (trigger instanceof SimpleQuartzTrigger) {
			SimpleQuartzTrigger sQT = (SimpleQuartzTrigger)trigger;
			exp.append(sQT.getStartTime() == null ? SIMPLE_EXPR_NOSTART : DateUtils.format(sQT.getStartTime()))
				.append(" ")
				.append(sQT.getEndTime() == null ? SIMPLE_EXPR_NOEND : DateUtils.format(sQT.getEndTime()))
				.append(" ")
				.append(sQT.getInterval())
				.append(" ")
				.append(sQT.getRepeats());
		} else {
			exp.append(((CronQuartzTrigger)trigger).getCronExpression());
		}
		
		Transaction trans = TransactionFactory.getTransaction();
		Iterator<Task> tasks = job.generateJobInstance().getTaskIterator();
		int ret = new InsertSql(getSqlTemplate())
				.inTransition(trans)
				.insert(TBLNAME_QJOB)
				.columns(COL_QJ_ID, COL_QJ_TRIGGER, COL_QJ_JOB, COL_QJ_EXPRESSION, COL_QJ_TYPE)
				.values(job.getJobId(), JsonSerilizer.serilize(trigger), jobFlow, exp.toString(), trigger.getTriggerType().getValue())
				.doInsert();
		if (ret > 0) {
			while (tasks.hasNext()) {
				Task task = tasks.next();
				ret += new InsertSql(getSqlTemplate())
						.inTransition(trans)
						.insert(TBLNAME_QTASK_PARAM)
						.columns(COL_QT_JID, COL_QT_ID)
						.values(job.getJobId(), task.getTaskId())
						.doInsert();
			}
		}
		
		try {
			if (ret > 0)
				trans.commit();
			else
				trans.rollback();
		} catch (Exception e) {
			ret = 0;
		} finally {
			try {
				trans.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return ret > 0;
	}
	
	protected boolean updateJobFlowExeTime(ServerQuartzJob job, Object date, Transaction trans) {
		int ret = new UpdateSql(getSqlTemplate())
				.inTransition(trans)
				.update()
				.table(TBLNAME_QJOB)
				.set(COL_QJ_LAST_EXETIME, date)
				.set(COL_QJ_STATUS, QJobStat.Run.getVal())
				.where(COL_QJ_ID + " = ?", job.getJobId().toString())
				.doUpdate();
		return ret > 0;
	}
	
	protected boolean addTaskBatchInstance(ServerQuartzJob job, Object date, Transaction trans) {
		Iterator<Task> it = job.generateJobInstance().getTaskIterator();
		int ret = 0, sum = 0;
		
		while (it.hasNext()) {
			Task task = it.next();
			ret += new InsertSql(getSqlTemplate())
					.inTransition(trans)
					.insert(TBLNAME_QTASK)
					.columns(COL_QT_ID, COL_QT_JID, COL_QT_EXETIME)
					.values(task.getTaskId(), job.getJobId(), date)
					.doInsert();
			sum += 1;
		}
		return ret == sum;
	}
	
	protected abstract Object transformTime(Date date);
	
	public boolean addJobFlowInstance(ServerQuartzJob job, Date date) {
		Object jDate = transformTime(date);
		Transaction trans = TransactionFactory.getTransaction();
		boolean ret = updateJobFlowExeTime(job, jDate, trans);
		if (ret) {
			ret = addTaskBatchInstance(job, jDate,trans);
		}
		try {
			if (ret)
				trans.commit();
			else
				trans.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
			ret = false;
		} finally {
			try {
				trans.close();
			} catch (SQLException ignore) {}
		}
		return ret;
	}
	
	
	public boolean updateTaskResult(String jobId, String taskId, Date date, TaskResult ret) {
		TaskStat stat = ret.getAction() == TaskAction.COMPLETE ? TaskStat.Finished : TaskStat.Fail;
		Transaction trans = TransactionFactory.getTransaction();
		ExecuteResult er = new ExecuteResult(ret.getKeyMessage(), ret.getAppendInfo());
		String erJson = JsonSerilizer.serilize(er);
		int num = new UpdateSql(getSqlTemplate())
				.inTransition(trans)
				.update()
				.table(TBLNAME_QTASK)
				.set(COL_QT_STAT, stat.getVal())
				.set(COL_QT_RESULT, erJson)
				.where(COL_QT_ID + " = ?", taskId)
				.and(COL_QT_EXETIME + " = ?", transformTime(date))
				.doUpdate();
		if (num > 0) {
			String param = JsonSerilizer.serilize(ret.getRuntimeParam());
			num += new UpdateSql(getSqlTemplate())
					.inTransition(trans)
					.update()
					.table(TBLNAME_QTASK_PARAM)
					.set(COL_QT_RUNTIME_PARAM, param)
					.where(COL_QT_ID + " = ?", taskId)
					.and(COL_QT_JID + " = ?", jobId)
					.doUpdate();	
		}
		
		try {
			if (num > 0)
				trans.commit();
			else
				trans.rollback();
		} catch (Exception e) {
			num = 0;
		} finally {
			try {
				trans.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return num > 0;
	}
	
	@Override
	public List<ServerQuartzJob> fetchQuartzJobs() {
		List<ServerQuartzJob> list = new LinkedList<ServerQuartzJob>();
		List<ServerQuartzJob> tmp = new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(TBLNAME_QJOB)
				.single(new QuartzHandler());
		for (ServerQuartzJob job : tmp) {
			Map<String, String> meta = this.fetchRuntimeParams(job.getJobId());
			Iterator<Map.Entry<String, String>> it = meta.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				String taskId = entry.getKey();
				String val = entry.getValue();
				if (val != null && !val.isEmpty()) {
					try {
						@SuppressWarnings("unchecked")
						Map<String, String> vals = new Gson().fromJson(val, Map.class);
						job.updateRuntimeParams(taskId, vals);	
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		list.addAll(tmp);
		return list;
	}
	
	@Override
	public ServerQuartzJob fetchQuartzJob(String jobId) {
		ServerQuartzJob job = null;
		List<ServerQuartzJob> tmp = new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(TBLNAME_QJOB)
				.where(COL_QJ_ID + " = ?", jobId)
				.single(new QuartzHandler());
		if (tmp != null && tmp.size() > 0)
			job = tmp.get(0);
		return job;
	}
	
	public boolean updateTaskStatsForJob(JobFlow job, Date date) {
		Map<String, SimpleTaskInfo> taskMap = getTaskInfo(job, date);
		Iterator<Task> it = job.getTaskIterator();
		while (it.hasNext()) {
			Task task = it.next();
			SimpleTaskInfo t = taskMap.get(task.getTaskId());
			if (t == null)
				return false;
			TaskStat stat = TaskStat.getInstance(t.statValue);
			task.markStatus(stat);
			task.updateExecuteResult(t.keymessage, t.feedback);
		}
		return true;
	}
	
	private Map<String, SimpleTaskInfo> getTaskInfo(JobFlow job, Date date) {
		Map<String,SimpleTaskInfo> ret = new SelectSql(getSqlTemplate())
				.select()
				.columns(COL_QT_ID, COL_QT_STAT, COL_QT_RESULT)
				.from()
				.table(TBLNAME_QTASK)
				.where(COL_QT_JID + " = ?", job.getJobId().toString())
				.and(COL_QT_EXETIME + " = ?", transformTime(date))
				.single(new QuartzTasksStatHandler());
		return ret;
	}

	public boolean updateJobStatus(String jobId, QJobStat stat) {
		try {
			int ret = new UpdateSql(getSqlTemplate())
					.update()
					.table(TBLNAME_QJOB)
					.set(COL_QJ_STATUS, stat.getVal())
					.where(COL_QJ_ID + " = ?", jobId)
					.doUpdate();
			return ret > 0;	
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean deleteQuartzJob(String jobId) {
		Transaction trans = TransactionFactory.getTransaction();
		int ret = 0;
		try {
			ret = new DeleteSql(getSqlTemplate())
				.inTransition(trans)
				.delete()
				.from()
				.table(TBLNAME_QJOB)
				.where(COL_QJ_ID + " = ?", jobId)
				.doDelete();
			if (ret > 0) {
				new DeleteSql(getSqlTemplate())
					.inTransition(trans)
					.delete()
					.from()
					.table(TBLNAME_QTASK)
					.where(COL_QT_JID + " = ?", jobId)
					.doDelete();
			}
			trans.commit();
		} catch (Exception e) {
			try {
				trans.rollback();
			} catch (Exception ignore) {
			}
			e.printStackTrace();
		} finally {
			try {
				trans.close();
			} catch (Exception ignore) {
			}
		}
		return ret > 0;
	}
	
	public Map<String, String> fetchRuntimeParams(String jobId) {
		Map<String, String> tasksMeta = new SelectSql(getSqlTemplate())
			.select()
			.columns(COL_QT_ID, COL_QT_RUNTIME_PARAM)
			.from()
			.table(TBLNAME_QTASK_PARAM)
			.where(COL_QT_JID + " = ?", jobId)
			.single(new RuntimeParamHandler());
		return tasksMeta;
	}
	
	public abstract boolean cleanDomain(String domain);

}
