package org.pbccrc.zsls.test.quartz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.pbccrc.zsls.api.client.old.SchedResult;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.api.quartz.builder.TriggerBuilder;
import org.pbccrc.zsls.api.thrift.utils.SchedClient;

public class QuartzTest {
	
	public static void main(String[] args) throws IOException {
		testSimple();
//		testCron();
		
		/*Date d = new Date();
		System.out.println(d);
		java.sql.Timestamp date = new java.sql.Timestamp(d.getTime());
		System.out.println(date);*/
	}
	
	public static String readFileContent(String file) throws IOException {
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
	
	public static void testSimple() throws IOException {
		String jobflow = readFileContent("src/test/resources/job1.xml");
		QuartzTrigger job = TriggerBuilder.newSimpleBuilder()
				.startNow()
				.withInterval(10, TimeUnit.SECONDS)
				.repeats(10000)
				.build();
		SchedClient client = new SchedClient("127.0.0.1:5555");
		SchedResult ret = client.send2Schedule(job, jobflow);
		System.out.println(ret);
	}
	
	public static void testCron() throws IOException {
		String jobflow = readFileContent("src/test/resources/job.xml");
		QuartzTrigger job = TriggerBuilder.newCronBuilder()
				.withExpression("0 0 23 * * ?")
				.build();
		SchedClient client = new SchedClient("127.0.0.1:5555");
		SchedResult ret = client.send2Schedule(job, jobflow);
		System.out.println(ret);
	}
	
}
