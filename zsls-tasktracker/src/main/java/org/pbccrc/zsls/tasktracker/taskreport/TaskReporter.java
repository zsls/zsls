package org.pbccrc.zsls.tasktracker.taskreport;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.pbccrc.zsls.api.thrift.InnerTrackerProtocol;
import org.pbccrc.zsls.api.thrift.records.HeartBeatRequest;
import org.pbccrc.zsls.api.thrift.records.HeartBeatResponse;
import org.pbccrc.zsls.api.thrift.records.ReportTaskRequest;
import org.pbccrc.zsls.api.thrift.records.ReportTaskResponse;
import org.pbccrc.zsls.api.thrift.records.TTaskResult;
import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.rpc.ZuesRPC;
import org.pbccrc.zsls.tasktracker.Controller;
import org.pbccrc.zsls.tasktracker.ServerLostListener;
import org.pbccrc.zsls.tasktracker.config.ClientConfig;
import org.pbccrc.zsls.tasktracker.config.RuntimeMeta;
import org.pbccrc.zsls.tasktracker.factory.ClientRecordFactory;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;

public class TaskReporter extends Thread {
	
	private static Logger L = Logger.getLogger(TaskReporter.class.getSimpleName());
	
	private InnerTrackerProtocol.Iface client;
	
	private BlockingQueue<TaskFinEvent> queue;
	
	private ReportTaskRequest cachedRequest;
	private ClientRecordFactory recordFactory;
	
	private ClientConfig config;
	private RuntimeMeta meta;
	
	private volatile InetSocketAddress serverAddr;
	
	private TaskReportCallback callback;
	private List<ServerLostListener> failListeners;
	
	private Controller controller;
	
	private volatile boolean pauseSend;
	
	public TaskReporter(ClientConfig config, Controller controller, ClientRecordFactory recordFactory) {
		super();
		this.setDaemon(true);
		this.config = config;
		this.callback = controller;
		this.controller = controller;
		this.pauseSend = true;
		this.recordFactory = recordFactory;
		this.meta = new RuntimeMeta(ZslsConstants.DEFAULT_HEART_BEAT_INTERVAL, 
				ZslsConstants.DEFAULT_LOAD_REGISTER_EXPIRE);
		queue = new LinkedBlockingQueue<TaskFinEvent>();
		failListeners = new ArrayList<ServerLostListener>();
	}
	
	public void updateServerAddr(InetSocketAddress addr, RuntimeMeta meta) {
		invalidAndCloseClient();
		if (addr != null) {
			serverAddr = addr;
			try {
				client = ZuesRPC.getRpcClient(InnerTrackerProtocol.Iface.class, addr);
			} catch (Exception e) {
			}
		}
		if (meta != null) {
			RuntimeMeta nmeta = new RuntimeMeta(meta.heartBeatInterval, meta.serverRegistryTimeout);
			this.meta = nmeta;	
		}
		pauseSend = false;
	}
	
	public void pauseSend() {
		pauseSend = true;
	}
	
	public void addToReport(TaskFinEvent event) {
		queue.add(event);
	}
	
	public void registerFailListener(ServerLostListener listener) {
		if (!failListeners.contains(listener))
			failListeners.add(listener);
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
		long startPoint = System.currentTimeMillis();
		while (true) {
			if (pauseSend) {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {}
				continue;
			}
			
			try {
				event = queue.poll(meta.heartBeatInterval, TimeUnit.MILLISECONDS);
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
				
			// send report request
			if (request != null) {
				ReportTaskResponse response = doReport(request);
				if (response == null) {
					cachedRequest = request;
					if (checkServerLost(startPoint))
						startPoint = System.currentTimeMillis();
				} else {
					startPoint = System.currentTimeMillis();
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
			// heart beat instead
			else {
				HeartBeatRequest heartbeat = recordFactory.getHeartBeat();
				HeartBeatResponse response = doHeartBeat(heartbeat);
				if (response == null) {
					if (checkServerLost(startPoint))
						startPoint = System.currentTimeMillis();
				} else {
					startPoint = System.currentTimeMillis();
					switch (response.getNodeAction()) {
					case NORMAL:
						break;
					default:
						L.error(response.getNodeAction() + " heart beat response received, would reregister");
						controller.pauseSystemAndReRegister();
						break;
					}
				}
			}
		}
	}
	
	private boolean checkServerLost(long startPoint) {
		if (System.currentTimeMillis() - startPoint > meta.serverRegistryTimeout) {
			for (ServerLostListener l : failListeners)
				l.onServerLost(serverAddr);
			return true;
		}
		return false;
	}

	private ReportTaskRequest makeRequest(TaskFinEvent event) {
		ReportTaskRequest request = recordFactory.genReportRequest(event.getContext(), config);
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
	
	private ReportTaskResponse doReport(ReportTaskRequest request) {
		ReportTaskResponse response = null;
		if (client != null) {
			try {
				response = client.taskComplete(request);
			} catch (TException e) {
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
		return response;
	}
	
	private HeartBeatResponse doHeartBeat(HeartBeatRequest request) {
		HeartBeatResponse response = null;
		if (client != null) {
			try {
				response = client.heartBeat(request);
			} catch (Exception e) {
			}
		}
		if (response == null) {
			invalidAndCloseClient();
			try {
				client = ZuesRPC.getRpcClient(InnerTrackerProtocol.Iface.class, serverAddr);
				response = client.heartBeat(request);
			} catch (Exception e) {
				L.error("failed to heartbeat to server " + serverAddr);
				invalidAndCloseClient();
			}
		}
		return response;
	}
	
}
