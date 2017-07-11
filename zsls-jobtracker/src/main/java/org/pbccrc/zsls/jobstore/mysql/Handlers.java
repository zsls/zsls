package org.pbccrc.zsls.jobstore.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.utils.JobUtils;
import org.pbccrc.zsls.api.quartz.CronQuartzTrigger;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.api.quartz.QuartzTrigger.TriggerType;
import org.pbccrc.zsls.api.quartz.SimpleQuartzTrigger;
import org.pbccrc.zsls.jobengine.Task.ExecuteResult;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.jobstore.JdbcJobStore;
import org.pbccrc.zsls.store.jdbc.utils.ResultSetHandler;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobFlow.RJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.tasks.rt.RTTask;
import org.pbccrc.zsls.utils.JsonSerilizer;
import org.pbccrc.zsls.utils.TaskUtil;

public class Handlers {
	
	public static class QuartzHandler implements ResultSetHandler<List<ServerQuartzJob>> {
		@Override
		public List<ServerQuartzJob> handle(ResultSet rs) throws SQLException {
			List<ServerQuartzJob> list = new LinkedList<ServerQuartzJob>();
			while (rs.next()) {
				String content = rs.getString(JdbcJobStore.COL_QJ_TRIGGER);
				QuartzTrigger trigger = null;
				TriggerType t = TriggerType.getInstance(rs.getInt(JdbcJobStore.COL_QJ_TYPE));
				if (t == TriggerType.CRONTAB)
					trigger = JsonSerilizer.deserilize(content, CronQuartzTrigger.class);
				else 
					trigger = JsonSerilizer.deserilize(content, SimpleQuartzTrigger.class);
				content = rs.getString(JdbcJobStore.COL_QJ_JOB);
				IJobFlow job = JobUtils.parseJobFlow(content);
				ServerQuartzJob sjob = new ServerQuartzJob(trigger, job);
				java.sql.Timestamp time = rs.getTimestamp(JdbcJobStore.COL_QJ_LAST_EXETIME);
				if (time != null)
					sjob.setLastExecuteTime(new Date(time.getTime()));
				int val = rs.getInt(JdbcJobStore.COL_QJ_STATUS);
				QJobStat stat = QJobStat.getJobStat(val);
				sjob.changeStatus(stat);
				sjob.setExpression(rs.getString(JdbcJobStore.COL_QJ_EXPRESSION));
				list.add(sjob);
			}
			return list;
		}
	}
	
	public static class SimpleTaskInfo {
		public int statValue;
		public String keymessage;
		public String feedback;
	}
	
	public static class QuartzTasksStatHandler  implements ResultSetHandler<Map<String, SimpleTaskInfo>> {

		@Override
		public Map<String, SimpleTaskInfo> handle(ResultSet rs) throws SQLException {
			Map<String, SimpleTaskInfo> result = new HashMap<String, SimpleTaskInfo>();
			while (rs.next()) {
				String taskId = rs.getString(JdbcJobStore.COL_QT_ID);
				SimpleTaskInfo task = new SimpleTaskInfo();
				task.statValue =  (int)rs.getLong(JdbcJobStore.COL_QT_STAT);
				ExecuteResult er = JsonSerilizer.deserilize(rs.getString(JdbcJobStore.COL_QT_RESULT), ExecuteResult.class);
				task.feedback = er.feedback;
				task.keymessage = er.keymessage;
				result.put(taskId, task);
			}
			return result;
		}
		
	}
	
	public static class UnitHandler implements ResultSetHandler<RTJobFlow> {
		@Override
		public RTJobFlow handle(ResultSet rs) throws SQLException {
			RTJobFlow sU = null;
			if (rs.next()) {
				IScheduleUnit iS = JsonSerilizer.deserilize(rs.getString(JdbcJobStore.COL_UNIT_CONTENT), IScheduleUnit.class);
				sU = TaskUtil.parseJobUnit(iS);
				sU.setUnitId(rs.getLong(JdbcJobStore.COL_UNIT_ID));
				sU.updateUnitIdForAllTasks(rs.getLong(JdbcJobStore.COL_UNIT_ID));
				int stat = rs.getInt(JdbcJobStore.COL_UNIT_STATUS);
				if (stat == RJobStat.Finished.getVal())
					sU.markJobFinish(true);
				return sU;
			}
			return null;
		}
	}
	
	public static class NumberHandler implements ResultSetHandler<Long> {
		@Override
		public Long handle(ResultSet rs) throws SQLException {
			long ret = 0L;
			if (rs.next()) {
				ret = rs.getLong(1);
			} else {
				return null;
			}
			return new Long(ret);
		}
	}
	
	public static class TaskIdListHandler implements ResultSetHandler<Set<String>> {
		@Override
		public Set<String> handle(ResultSet rs) throws SQLException {
			Set<String> list = new HashSet<String>(256);
			while (rs.next()) {
				String taskid = rs.getString(1);
				list.add(taskid);
			}
			return list;
		}
	}
	
	public static class BatchUnitHandler implements ResultSetHandler<List<RTJobFlow>> {
		@Override
		public List<RTJobFlow> handle(ResultSet rs) throws SQLException {
			List<RTJobFlow> sList = new LinkedList<RTJobFlow>();
			while (rs.next()) {
				long id = rs.getLong(JdbcJobStore.COL_UNIT_ID);
				//IScheduleUnit iS = JsonSerilizer.deserilize(rs.getString(JdbcJobStore.COL_UNIT_CONTENT), IScheduleUnit.class);
				//RTJobFlow unit = TaskUtil.parseJobUnit(iS);
				RTJobFlow unit = new RTJobFlow();
				Date date = rs.getDate(JdbcJobStore.COL_UNIT_CTIME);
				unit.setUnitId(id);
				unit.setGenerateTime(date);
				long preUnitId = rs.getLong(JdbcJobStore.COL_UNIT_PREUNIT);
				unit.setPreUnit(preUnitId > 0 ? new RTJobId(preUnitId) : null);
				int stat = rs.getInt(JdbcJobStore.COL_UNIT_STATUS);
				if (stat == RJobStat.Finished.getVal())
					unit.markJobFinish(true);
				//unit.updateUnitIdForAllTasks(id);
				sList.add(unit);
			}
			return sList;
		}
	}
	
	public static class BatchTaskHandler implements ResultSetHandler<Map<String, RTTask>> {
		@Override
		public Map<String, RTTask> handle(ResultSet rs) throws SQLException {
			Map<String, RTTask> uList = new HashMap<String, RTTask>();
			while (rs.next()) {
				String taskId = rs.getString(JdbcJobStore.COL_TASK_ID);
				TaskStat stat = TaskStat.getInstance(rs.getInt(JdbcJobStore.COL_TASK_STATUS));
				RTTask task = new RTTask(taskId, null);
				task.markStatus(stat);
				String info = rs.getString(JdbcJobStore.COL_TASK_INFO);
				ExecuteResult rt = JsonSerilizer.deserilize(info, ExecuteResult.class);
				if (rt != null)
					task.updateExecuteResult(rt.keymessage, rt.feedback);
				uList.put(taskId, task);
			}
			return uList;
		}
	}
	
	public static class RuntimeParamHandler implements ResultSetHandler<Map<String, String>> {

		@Override
		public Map<String, String> handle(ResultSet rs) throws SQLException {
			Map<String, String> tasksMeta = new HashMap<String, String>();
			while (rs.next()) {
				String taskId = rs.getString(JdbcJobStore.COL_QT_ID);
				String taskMeta = rs.getString(JdbcJobStore.COL_QT_RUNTIME_PARAM);
				tasksMeta.put(taskId, taskMeta);
			}
			return tasksMeta;
		}
		
	}

}
