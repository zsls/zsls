package org.pbccrc.zsls.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.old.IReEntry;
import org.pbccrc.zsls.api.client.old.IRelation;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.old.IUserTask;
import org.pbccrc.zsls.api.client.utils.JobUtils;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.api.quartz.builder.TriggerBuilder;
import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.DbConfig;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobstore.JobStore;
import org.pbccrc.zsls.jobstore.mysql.MysqlJobStore;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;

public class TestJobStore
{
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	public static void main(String[] args) throws Exception {
		//testStoreQZJobFlow();
		testAddJobFlowInstance();
		//testUpdateExecuteResult();
		//testUpdateTasksForJob();
		//testUpdateJobStatus();
		//testFetchQuartzJobs();
		//IScheduleUnit iUnit = genUnit(domain);
		//ScheduleUnit unit = TaskUtil.parseSchedule(iUnit);
		//RTJobFlow su = jobStore.fetchUnit(domain, new RTJobId(1));
				//.getFirstUnfinishedUnit();
		//System.out.println("unitID: " + su.getDomain());
	}
	
	public static JobStore testMysqlJobStore() throws IOException {
		Configuration conf = new Configuration();
		conf.load();
		DbConfig dbConfig = DbConfig.readConfig(conf);
		return new MysqlJobStore(dbConfig);
	}
	
	public static String genJobFlowXML() throws IOException {
		StringBuilder b = new StringBuilder();
		BufferedReader reader = new BufferedReader(new FileReader(new File("src/test/resources/job.xml")));
		String line = null;
		while ((line = reader.readLine()) != null) {
			b.append(line);
		}
		reader.close();
		String in = b.toString();
		return in;
	}
	public static void testStoreQZJobFlow() throws Exception {
		JobStore jobStore = testMysqlJobStore();
		
		//QuartzTrigger qT = new SimpleQuartzTrigger();
		/*QuartzTrigger trigger = TriggerBuilder.newSimpleBuilder()
				.jobData("key", "val1")
				.startAt(sdf.parse("2016-08-19 00:00:00"))
				.endTime(sdf.parse(sdf.format(new Date())))
				.repeats(2)
				.withInterval(30, TimeUnit.SECONDS)
				.build();*/
		QuartzTrigger trigger = TriggerBuilder.newCronBuilder()
				.jobData("key", "val1")
				.withExpression("0 0 23 * * ?")
				.build();
		//IJobFlow iJobFlow = EngineTest.genIJobFlow();
		String in = genJobFlowXML();
		IJobFlow ijob = JobUtils.parseJobFlow(in);
		jobStore.storeQuartzJob(new ServerQuartzJob(trigger, ijob), trigger, in);
	}
	
	public static void testAddJobFlowInstance() throws IOException, ParseException {
		JobStore jobStore = testMysqlJobStore();
		QuartzTrigger trigger = TriggerBuilder.newSimpleBuilder()
				.jobData("key", "val1")
				.startAt(sdf.parse("2016-08-20 10:00:00.000"))
				.endTime(sdf.parse(sdf.format(new Date())))
				.repeats(2)
				.withInterval(30, TimeUnit.SECONDS)
				.build();
		ServerQuartzJob sQJ = new ServerQuartzJob(trigger, JobUtils.parseJobFlow(genJobFlowXML()));
		
		Date date = new Date(System.currentTimeMillis());
		System.out.println("Date: " + sdf.format(date));
		jobStore.addJobFlowInstance(sQJ, date);
	}
	
	public static void testUpdateExecuteResult() throws IOException, ParseException {
		JobStore jobStore = testMysqlJobStore();
		TaskResult tr = new TaskResult();
		tr.setAction(TaskAction.QUARTZ_COMPLETE);
		tr.setAppendInfo("IT NOT OK");
		jobStore.updateTaskResult("id-test1", "id-task-1", sdf.parse("2016-08-19 00:00:00"), tr);
	}
	
	public static void testUpdateTasksForJob() throws IOException, ParseException {
		JobStore jobStore = testMysqlJobStore();
		List<ServerQuartzJob> list = jobStore.fetchQuartzJobs();
		JobFlow job = list.get(0).generateJobInstance();
		jobStore.updateTaskStatsForJob(job, sdf.parse("2016-08-19 00:00:00"));
		Iterator<Task> it = job.getTaskIterator();
		while (it.hasNext()) {
			Task task = it.next();
			System.out.println(task.getTaskId() + " : " + task.getStatus() + " : " + (task.getResultMsg() == null ? null : task.getResultMsg().keymessage));
		}
	}
	
	public static void testFetchQuartzJobs() throws IOException {
		JobStore jobStore = testMysqlJobStore();
		List<ServerQuartzJob> list = jobStore.fetchQuartzJobs();
		System.out.println("jobNum:" + list.size());
	}
	
	static void testUpdateJobStatus() throws IOException {
		JobStore jobStore = testMysqlJobStore();
		jobStore.updateJobStatus("id-test1", QJobStat.Run);
	}
	public static IScheduleUnit genUnit(String domain) {
		
		IUserTask t1 = new IUserTask("1");
		t1.params.put("param1", "PARAM1");
		IUserTask t2 = new IUserTask("2");
		t2.params.put("param2", "PARAM2");
		IUserTask t3 = new IUserTask("3");
		t3.params.put("param3", "PARAM3");
		IUserTask t4 = new IUserTask("4");
		t4.params.put("param4", "PARAM4");
		IUserTask t5 = new IUserTask("5");
		t5.params.put("param5", "PARAM5");
		IUserTask t6 = new IUserTask("6");
		t6.params.put("param6", "PARAM6");
		
		IRelation relation = new IRelation();
		IReEntry e = new IReEntry("1");
		e.tasks.add(t1);
		relation.preTasks = e;
		e = new IReEntry("2");
		e.tasks.add(t2);
		relation.postTasks= e;
		
		//IRelation r2 = new IRelation();
		/*r2.preTasks = e;
		e = new IReEntry("3");
		e.tasks.add(t5);
		e.tasks.add(t6);
		r2.postTasks = e;*/

		IScheduleUnit unit = new IScheduleUnit(domain);
		unit.swiftNum = domain + "1";
		unit.relations.add(relation);
		unit.independentTasks.add(t3);
		unit.independentTasks.add(t4);
		unit.independentTasks.add(t5);
		unit.independentTasks.add(t6);
		unit.timeout = 20000;
		unit.preUnit = "-1";
		/*String preUnitId = DBHelperTest.testGetDBLastUndoneData(domain);
		System.out.println("#########preUnitId : " + preUnitId);
		if (preUnitId != null) {
			unit.preUnit = preUnitId;
		}*/
		return unit;
	}

}
