package org.pbccrc.zsls.tasktracker.stub.job;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class StubNormalJob {
	
	public static void main(String[] args) throws InterruptedException {
		long time = 60 * 1000;
		if (args.length > 1 && args[0].equals("-t"))
			time = Long.parseLong(args[1]) * 1000;
		System.out.println("StubNormalJob start... sleep time(seconds) -> " + time);
		
		Thread.sleep(time);
		
		// runtime parameters
		Map<String, String> map = new HashMap<String, String>();
		map.put("time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
		System.out.print("%SCHED_META{");
		System.out.print(new Gson().toJson(map));
		System.out.println("}%");
		
		// key message
		System.out.println("%SCHED_MSG{key_message}%");
	}

}
