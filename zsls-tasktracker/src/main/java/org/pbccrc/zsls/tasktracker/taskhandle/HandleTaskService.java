package org.pbccrc.zsls.tasktracker.taskhandle;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.pbccrc.zsls.api.thrift.TaskHandleProtocol;
import org.pbccrc.zsls.api.thrift.records.TTaskId;
import org.pbccrc.zsls.api.thrift.records.TTaskInfo;
import org.pbccrc.zsls.api.thrift.records.TaskHandleRequest;
import org.pbccrc.zsls.api.thrift.records.TaskType;
import org.pbccrc.zsls.rpc.ZuesRPC;
import org.pbccrc.zsls.rpc.server.Server;
import org.pbccrc.zsls.tasktracker.ClientContext;
import org.pbccrc.zsls.tasktracker.RegInfo.RegStatus;
import org.pbccrc.zsls.tasktracker.config.ClientConfig;
import org.pbccrc.zsls.tasktracker.taskhandle.handler.ShellTaskExecutor;

public class HandleTaskService implements TaskHandleProtocol.Iface {
	
	private static Logger L = Logger.getLogger(HandleTaskService.class.getSimpleName());
	
	private static ThreadLocal<TaskHandler> tlHandler = new ThreadLocal<TaskHandler>();
	
	private ClientConfig config;
	
	private Server server;
	
	private ExecutorService executor;
	
	private TaskFinishCallback callback;
	
	private ClientContext context;
	
	private TaskHandler globalTaskHandler;
	
	private TaskContextFactory cxtFactory;
	
	public HandleTaskService (ClientConfig config, TaskFinishCallback callback, ClientContext context) {
		this.config = config;
		this.callback = callback;
		this.context = context;
		executor = Executors.newCachedThreadPool();
		cxtFactory = new TaskContextFactory(context);
		assert(constructTaskHandler(config.getTaskHandler()) != null);
	}
	
	public void start() throws Exception {
		int ioThreads = config.getIothreads() > 0 ? config.getIothreads() : 4;
		InetSocketAddress addr = new InetSocketAddress("127.0.0.1", config.getRecvPort());
		server = ZuesRPC.getRpcServer(TaskHandleProtocol.Iface.class, 
				this, addr, ioThreads, 0);
		server.start();
		L.info("######## handle task service started on port: " + config.getRecvPort());
	}
	
	public void stop() {
		server.stop();
	}
	
	public void setGlobalTaskHandler(TaskHandler handler) {
		this.globalTaskHandler = handler;
	}
	
	public TaskContextFactory getTaskCxtFactory() {
		return cxtFactory;
	}
	
	@Override
	public void assignTask(TaskHandleRequest request) throws TException {
		TaskType type = request.getTaskType();
		TTaskInfo taskInfo = request.getTaskInfo();
		TTaskId taskId = request.getTaskInfo().getTaskid();
		L.info("receive new task of type " + type + ", id: " + taskId);
		if (context.getRegInfo().getStatus() == RegStatus.Lost) {
			L.warn("reject newly received task " + taskId + 
					" since we are lost, probalby we are restarting...");
			return;
		}
		TaskHandler handler = getTaskHandler();
		if (handler != null) {
			TaskContext cxt = cxtFactory.newTaskContext(type, taskInfo, request.isRetryTask());
			Task task = new Task(handler, cxt);
			context.getTaskManager().registerTask(cxt.getTaskDetail().getTaskId(), cxt);
			executor.execute(task);
		} else {
			L.error("TaskHandler is null");
		}
	}
	
	public void addToRetry(TaskContext context) {
		cxtFactory.updateContextForRetry(context);
		TaskHandler handler = getTaskHandler();
		Task task = new Task(handler, context);
		executor.execute(task);
	}
	
	public class Task implements Runnable {
		TaskHandler handler;
		TaskContext context;
		Task(TaskHandler handler, TaskContext context) {
			this.handler = handler;
			this.context = context;
		}
		@Override
		public void run() {
			boolean ret = false;
			String taskid = context.getTaskDetail().getTaskId();
			try {
//				L.info("start handle task " + taskid);
				ret = handler.handleTask(context);
			} catch (Exception e) {
				L.error("exception during task execution: " + e);
				ret = false;
			}
			TaskExecutionInfo info = HandleTaskService.this.context
						.getTaskManager().getTaskExecutionInfo(taskid);
			if (ret) {
				L.info("task " + taskid + " completed");
				callback.onTaskComplte(info);
			} else {
				L.info("task " + taskid + " failed");
				callback.onTaskFail(info);
			}
		}
	}
	
	private TaskHandler getTaskHandler() {
		if (globalTaskHandler != null)
			return globalTaskHandler;
		else {
			TaskHandler handler = tlHandler.get();
			if (handler == null) {
				handler = constructTaskHandler(config.getTaskHandler());
				tlHandler.set(handler);
			}
			return handler;
		}
	}
	
	private TaskHandler constructTaskHandler(String handlerName) {
		try {
			@SuppressWarnings("unchecked")
			Class<TaskHandler> clazz = (Class<TaskHandler>) Class.forName(handlerName);
			Constructor<TaskHandler> constructor = clazz.getConstructor();
			TaskHandler handle = constructor.newInstance();
			if (handle instanceof ShellTaskExecutor)
				((ShellTaskExecutor)handle).setClientContext(context);
			handle.init();	
			return handle;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void killTask(TaskHandleRequest request) throws TException {
		TTaskId taskId = request.getTaskInfo().getTaskid();
		L.info("kill task " + taskId);	
		TaskExecutionInfo info = context.getTaskManager().getTaskExecutionInfo(taskId.taskid);
		if (info != null && info.shell != null) {
			if (info.shell.kill()) {
				L.info("task " + taskId.taskid + " killed");
				return;
			}
		} else
			L.error("No task " + taskId.taskid + " or not shell task type");
	}
	
}
