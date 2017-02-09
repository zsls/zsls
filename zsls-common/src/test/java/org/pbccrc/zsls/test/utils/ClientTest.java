package org.pbccrc.zsls.test.utils;

import java.io.IOException;

import org.pbccrc.zsls.api.thrift.utils.SchedClient;

public class ClientTest {
	
	public static void main(String[] args) throws IOException {
		testRunningNum();
	}
	
	public static void testRunningNum() throws IOException {
		SchedClient client = new SchedClient("127.0.0.1:5555");
		int num = client.getRunningUnitNum("DATA_LOAD");
		System.out.println("running unit num: " + num);
	}
	
}
