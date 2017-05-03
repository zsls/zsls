package org.pbccrc.zsls.test;

import java.net.InetSocketAddress;
import java.util.HashMap;

import org.pbccrc.zsls.api.thrift.TaskHandleProtocol;
import org.pbccrc.zsls.api.thrift.records.TTaskInfo;
import org.pbccrc.zsls.api.thrift.records.TaskHandleRequest;
import org.pbccrc.zsls.api.thrift.records.TaskType;
import org.pbccrc.zsls.api.thrift.utils.RecordUtil;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskInfo;
import org.pbccrc.zsls.rpc.ZuesRPC;

public class ThriftClientTest {
	
	public static void main(String[] args) throws Exception {
		testConnection();
	}
	
	public static void testConnection() throws Exception {
		TaskHandleProtocol.Iface client = ZuesRPC.getRpcClient(TaskHandleProtocol.Iface.class, 
					new InetSocketAddress("127.0.0.1", 2609));
		TaskHandleRequest request = new TaskHandleRequest();
		
		request.setTaskType(TaskType.NORMAL);
		TaskInfo task = new TaskInfo();
		task.setTaskId(new TaskId("task-id"));
		task.setData(new HashMap<String, String>());
		task.setGenerateTime(System.currentTimeMillis());
		TTaskInfo info = RecordUtil.trans(task);
		request.setTaskInfo(info);
		
		client.assignTask(request);
		System.out.println("client send a request");
		client.assignTask(request);
		System.out.println("client send a request");
		
		System.out.println("now client sleep for 10 seconds");
		Thread.sleep(1000000L);
		client.assignTask(request);
//		System.out.println("client send a request");
		
		System.out.println("close client");
		ZuesRPC.closeClient(client);
	}

}
