package org.pbccrc.zsls.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.pbccrc.zsls.api.client.IConvergeGateway;
import org.pbccrc.zsls.api.client.IDataFlow;
import org.pbccrc.zsls.api.client.IDivergeGateway;
import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.ITask;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.utils.JobUtils;
import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.front.request.utils.ParamValidator;
import org.pbccrc.zsls.jobengine.JobEngine;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.JobManager;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.utils.TaskUtil;

public class EngineTest {
	
	public static void main(String[] args) throws IOException {
//		IJobFlow iJobFlow = genIJobFlow();
		IJobFlow iJobFlow = readXMLJobFlow();
		if (!ParamValidator.checkValid(iJobFlow))
			return ;
		JobFlow job = TaskUtil.parseJobFlow(iJobFlow);
		doJob(job);
	}
	
	static String readFileContent(String file) throws IOException {
		StringBuilder b = new StringBuilder();
		BufferedReader reader = new BufferedReader(new FileReader(new File(file)));
		String line = null;
		while ((line = reader.readLine()) != null) {
			b.append(line);
		}
		reader.close();
		String in = b.toString();
		return in;
	}
	
	static IJobFlow readXMLJobFlow() throws IOException {
		String in = readFileContent("src/test/resources/job.xml");
		IJobFlow job = JobUtils.parseJobFlow(in);
		return job;
	}
	
	static void doJob(JobFlow job) {
		Iterator<Task> it = job.getTaskIterator();
		while (it.hasNext()) {
			Task t = it.next();
			System.out.println(t.getTaskId());
		}
//		job.setJobId(new RTJobId(100L));
		JobEngine engine = new JobEngine(new JobManager());
		engine.feed(job);
		ArrayList<Task> list = new ArrayList<Task>();
		int i = 0;
		while (true) {
			i++;
			Task task = null;
			while ((task = engine.next()) != null) {
				list.add(task);
				System.out.println("time " + i + " -> " + task.getTaskId());
			}
			if (list.size() == 0)
				break;
			for (Task t : list) {
				TaskResult ret = new TaskResult();
				ret.setAction(TaskAction.COMPLETE);
				ret.setTaskId(new TaskId(t.getTaskId()));
				engine.complete(new TaskId(t.getTaskId()), ret);
			}	
			list.clear();
		}
		System.out.println(job.isFinished());
	}
	
	static void test() {
		IScheduleUnit unit = CommunicationTest.genUnit("tmp_domain");
		JobFlow job = TaskUtil.parseJobUnit(unit);
		doJob(job);
	}
	
	static IJobFlow genIJobFlow() {
		IJobFlow jobFlow = new IJobFlow();
		jobFlow.id = "job1";
		ITask t1 = new ITask();
		t1.id = "t1";
		t1.domain = "DOMAIN_DF";
		t1.params = new HashMap<String, String>();
		t1.params.put("CODE", "OK");
		
		ITask t2 = new ITask();
		t2.id = "t2";
		t2.domain = "DOMAIN_DF";
		t2.params = new HashMap<String, String>();
		t2.params.put("CODE", "OK");
		
		IDivergeGateway d1 = new IDivergeGateway();
		d1.id = "d1";
		IConvergeGateway c1 = new IConvergeGateway();
		c1.id = "c1";
		
		IDataFlow df1 = new IDataFlow();
		df1.source = "START";
		df1.target = "d1";
		
		
		IDataFlow df2 = new IDataFlow();
		df2.source = "d1";
		df2.target = "t1";
		
		
		IDataFlow df3 = new IDataFlow();
		df3.source = "d1";
		df3.target = "t2";
		
		
		IDataFlow df4 = new IDataFlow();
		df4.source = "t1";
		df4.target = "c1";
		
		IDataFlow df5 = new IDataFlow();
		df5.source = "t2";
		df5.target = "c1";
		
		IDataFlow df6 = new IDataFlow();
		df6.source = "c1";
		df6.target = "END";
		
		jobFlow.dataFlows = new ArrayList<IDataFlow>();
		jobFlow.dataFlows.add(df1);
		jobFlow.dataFlows.add(df2);
		jobFlow.dataFlows.add(df3);
		jobFlow.dataFlows.add(df4);
		jobFlow.dataFlows.add(df5);
		jobFlow.dataFlows.add(df6);
		
		jobFlow.flowObjs = new ArrayList<Object>();;
		jobFlow.flowObjs.add(d1);
		jobFlow.flowObjs.add(t1);
		jobFlow.flowObjs.add(t2);
		jobFlow.flowObjs.add(c1);
		
		return jobFlow;
	}

}
