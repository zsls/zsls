package org.pbccrc.zsls.tasktracker.taskreport;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.pbccrc.zsls.api.thrift.InnerTrackerProtocol;
import org.pbccrc.zsls.api.thrift.records.ReportTaskRequest;
import org.pbccrc.zsls.api.thrift.records.ReportTaskResponse;
import org.pbccrc.zsls.api.thrift.records.TTaskResult;
import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.rpc.ZuesRPC;
import org.pbccrc.zsls.tasktracker.Controller;
import org.pbccrc.zsls.tasktracker.config.ClientConfig;
import org.pbccrc.zsls.tasktracker.factory.ClientRecordFactory;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;

public class TaskReporter extends Thread {
	
	private static Logger L = Logger.getLogger(TaskReporter.class.getSimpleName());
	
	private InnerTrackerProtocol.Iface client;
	
	private BlockingQueue<TaskFinEvent> queue;
	
	private ReportTaskRequest cachedRequest;
	
	private ClientConfig config;
	
	private volatile InetSocketAddress serverAddr;
	
	private TaskReportCallback callback;
	
	private Controller controller;
	
	private volatile boolean pauseSend;
	
	public TaskReporter(ClientConfig config, Controller controller) {
		super();
		this.setDaemon(true);
		this.config = config;
		this.callback = controller;
		this.controller = controller;
		this.pauseSend = true;
		queue = new LinkedBlockingQueue<TaskFinEvent>();
	}
	
	public void updateServerAddr(InetSocketAddress addr) {
		invalidAndCloseClient();
		if (addr != null) {
			serverAddr = addr;
			pauseSend = false;
			try {
				client = ZuesRPC.getRpcClient(InnerTrackerProtocol.Iface.class, addr);
			} catch (Exception e) {
			}
		}
	}
	
	public void pauseSend() {
		pauseSend = true;
	}
	
	public void addToReport(TaskFinEvent event) {
		queue.add(event);
	}
	
	private void invalidAndCloseClient() {
		if (client != null) {
			InnerTrackerProtocol.Iface cli = client;
			client = null;
			ZuesRPC.closeClient(cli);	
		}
	}
	
	public void start() {
		super.start();
	}
	
	public void run() {
		TaskFinEvent event = null;
		while (true) {
			try {
				event = queue.poll(2000L, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ignore) {
			}
			
			// merge with cached requests
			ReportTaskRequest request = null;
			if (event != null) {
				request = makeRequest(event);
				if (cachedRequest != null) {
					for (TTaskResult result : cachedRequest.getTaskResults())
						request.getTaskResults().add(result);
				}
			} else {
				request = cachedRequest;
			}
				
			// send
			if (request != null) {
				if (pauseSend)
					cachedRequest = request;
				else
					doSend(request);
			}
		}
	}

	private ReportTaskRequest makeRequest(TaskFinEvent event) {
		ReportTaskRequest request = ClientRecordFactory.genReportRequest(event.getContext(), config);
		switch (event.getType()) {
		case Completed:
			TaskAction action = TaskAction.COMPLETE;
			for (TTaskResult result : request.getTaskResults())
				result.setAction(action);
			break;
		case Fail:
			action = TaskAction.FAILED;
			for (TTaskResult result : request.getTaskResults())
				result.setAction(action);
			break;
		}
		return request;
	}
	
	private void doSend(ReportTaskRequest request) {
		ReportTaskResponse response = null;
		if (client != null) {
			try {
				response = client.taskComplete(request);
			} catch (TException ignore) {
			}
		}
		if (response == null) {
			invalidAndCloseClient();
			try {
				client = ZuesRPC.getRpcClient(InnerTrackerProtocol.Iface.class, serverAddr);
				response = client.taskComplete(request);	
			} catch (Exception e) {
				StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
				b.append("failed to report tasks [");
				for (TTaskResult re: request.getTaskResults()) {
					b.append(re.getTaskid().getTaskid()).append(", ");
				}
				b.append("] to server ").append(serverAddr).append(": ").append(e);
				L.error(b.toString());
				invalidAndCloseClient();
			}
		}
		
		if (response == null) {
			cachedRequest = request;
			return;
		}
		
		switch (response.getNodeAction()) {
		case INVALID:
			cachedRequest = request;
			L.warn("invalid response of task report received, probably server error");
			break;
		case NORMAL:
			cachedRequest = null;
			if (callback != null) {
				for (TTaskResult r : request.getTaskResults()) {
					String taskId = r.getTaskid().getTaskid();
					callback.onTaskReported(taskId);	
				}
			}
			break;
		case NOT_MASTER:
		case RE_REGISTER:
			cachedRequest = request;
			L.warn(response.getNodeAction() + " response of task report received, would reregister");
			controller.pauseSystemAndReRegister();
			break;
		default:
			break;
		}
		
	}
	
}
