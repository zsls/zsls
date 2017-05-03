package org.pbccrc.zsls.tasktracker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.exception.ZslsRuntimeException;
import org.pbccrc.zsls.tasktracker.config.ClientConfig;
import org.pbccrc.zsls.tasktracker.factory.ClientRecordFactory;
import org.pbccrc.zsls.tasktracker.register.RegisterManager;
import org.pbccrc.zsls.tasktracker.register.RegisterResult;
import org.pbccrc.zsls.tasktracker.taskhandle.HandleTaskService;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskHandler;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskManager;
import org.pbccrc.zsls.tasktracker.taskhandle.handler.ShellTaskExecutor;
import org.pbccrc.zsls.tasktracker.taskreport.TaskReporter;
import org.pbccrc.zsls.utils.FileUtils;
import org.pbccrc.zsls.utils.LocalUtils;
import org.pbccrc.zsls.utils.NumberUtils;
import org.pbccrc.zsls.utils.Shell;
import org.pbccrc.zsls.utils.Shell.ShellCommandExecutor;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TaskTracker {
	
	private static Logger L = Logger.getLogger(TaskTracker.class.getSimpleName());
	
	private TaskReporter reporter;
	private HandleTaskService service;
	private RegisterManager regManager;
	
	private Controller controller;
	
	private ClientRecordFactory factory;
	
	private ClientConfig config;
	private ClientContext context;
	
	public TaskTracker(ClientConfig config) {
		L.info("######### config detail: " + config);
		this.config = config;
		this.context = new ClientContextImpl();
		context.setConfig(config);
		context.setTaskManager(new TaskManager());
		factory = new ClientRecordFactory(context);
	}
	
	public TaskTracker(ClientConfig config, TaskHandler taskHandler) {
		L.info("######### config detail: " + config);
		this.config = config;
		this.context = new ClientContextImpl();
		context.setConfig(config);
		context.setTaskManager(new TaskManager());
		if (taskHandler != null) {
			if (!taskHandler.init())
				throw new ZslsRuntimeException("failed to init TaskHandler");
			context.setGlobalTaskHandler(taskHandler);
		}
		factory = new ClientRecordFactory(context);
	}
	
	private void init() throws Exception {
		// controller
		controller = new Controller(context);
		
		// register manager
		String serverAddrs = config.getServerAddrs();
		List<InetSocketAddress> addrs = LocalUtils.getAddrs(serverAddrs);
		regManager = new RegisterManager(factory, addrs);
		context.setRegisterManager(regManager);
		
		// task reporter
		reporter = new TaskReporter(config, controller, factory);
		reporter.registerFailListener(controller);
		context.setTaskReporter(reporter);
		
		// task_handle server
		service = new HandleTaskService(config, controller, context);
		if (context.getGlobalTaskHandler() != null)
			service.setGlobalTaskHandler(context.getGlobalTaskHandler());
		context.setHandleTaskService(service);
	}
	
	public void start() throws Exception {
		// init
		init();
		preClean();
		
		// start
		service.start();
		reporter.start();
		
		// try initial register
		RegisterResult ret = regManager.tryRegister();
		if (ret == null) {
			service.stop();
			throw new IOException("cannot connect to servers...");
		} else {
			controller.onRegistered(ret);
		}
	}
	
	private void preClean() throws Exception {
		String path = ShellTaskExecutor.DIR_RUN ;
		File file = new File(path);
		if (file.exists() && file.isDirectory()) {
			for (String taskId : file.list()) {
				String tpath = path + "/" + taskId;
				File pidFile = new File(tpath + "/" + taskId + ShellTaskExecutor.APPEND_PID);
				if (pidFile.exists() && !pidFile.isDirectory()) {
					String pid = null;
					BufferedReader reader = new BufferedReader(new FileReader(pidFile));
					try {
						pid = reader.readLine();
					} finally {
						try {reader.close();}
						catch (Exception ignore) {}
					}
					if (NumberUtils.isNumber(pid)) {
						ShellCommandExecutor exc = new ShellCommandExecutor(
								Shell.getSignalKillCommand(9, pid));
						exc.execute();
						if (exc.getExitCode() == 0) {
							L.info("killed process " + pid);
							FileUtils.delete(new File(tpath));
						} else {
							L.error("failed to kill process " + pid);
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:*.xml");
		context.start();
		ClientConfig config = (ClientConfig)context.getBean("clientConfig");
		TaskTracker client = new TaskTracker(config);
		client.start();
		context.close();
	}

}
