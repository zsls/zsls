package org.pbccrc.zsls.test.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.utils.JobUtils;

import com.google.gson.Gson;
import com.thoughtworks.xstream.XStream;

public class JsonTest {
	
	public static void main(String[] args) throws Exception {
//		testXStream();
//		test1();
		testGson();
	}
	
	@SuppressWarnings("unchecked")
	public static void testGson() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("key", "value");
		Gson gson = new Gson();
		String json = gson.toJson(map);
		System.out.println(json);
		map = gson.fromJson(json, Map.class);
		System.out.println(map);
	}
	
	static class Entry {
		public String id = "a";
		public String obj = null;
	}
	
	public static void testXStream() throws Exception {
		StringBuilder b = new StringBuilder();
		BufferedReader reader = new BufferedReader(new FileReader(new File("src/test/resources/job.xml")));
		String line = null;
		while ((line = reader.readLine()) != null) {
			b.append(line);
		}
		reader.close();
		String in = b.toString();
		IJobFlow flow = JobUtils.parseJobFlow(in);
		System.out.println(flow.id);
	}
	
	static class E {
		public String name;
	}
	
	public static void test1() throws Exception {
		String in = "<O><name>abc</name></O>";
		XStream stream = new XStream();
		stream.alias("O", E.class);
		Object o = stream.fromXML(in);
		System.out.println(o);
	}

}
