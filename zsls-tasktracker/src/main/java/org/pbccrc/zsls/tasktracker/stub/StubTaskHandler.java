package org.pbccrc.zsls.tasktracker.stub;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.tasktracker.taskhandle.ResultWriter;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskContext;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskDetail;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskHandler;

public class StubTaskHandler implements TaskHandler {
	protected static Logger L = Logger.getLogger(StubTaskHandler.class.getSimpleName());
	
	private Random random;

	@Override
	public boolean init() {
		random = new Random();
		return true;
	}
	
	@Override
	public boolean handleTask(TaskContext context) {
		try {
			TaskDetail task = context.getTaskDetail();
			Map<String, String> data = task.getParams();
			// do task
			System.out.println("### doing task " + task.getTaskId() + ", jobTime: " + task.getJobTime());
			for (String k : data.keySet()) {
				System.out.println("\t param: " + k + " -> " + data.get(k));
			}
			long executeTime = (random.nextInt(10) + 10) * 100;
			Thread.sleep(executeTime);
			
			ResultWriter writer = context.getResultWriter();
			writer.writeFeedbackMessage("i'm feedback message");	// feedback message
			writer.writeKeyMessage("key message");					// key message
			Map<String, String> map = new HashMap<String, String>();
			map.put("startTimes", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
			writer.updateRuntimeParams(map);						// runtime parameters
			return false;
		} catch (Exception ignore) {
			ignore.printStackTrace();
			return false;
		}
	}

}