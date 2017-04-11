package org.pbccrc.zsls.tasktracker;

import org.pbccrc.zsls.tasktracker.RegInfo.RegStatus;
import org.pbccrc.zsls.tasktracker.config.ClientConfig;
import org.pbccrc.zsls.tasktracker.config.RuntimeMeta;
import org.pbccrc.zsls.tasktracker.register.RegisterManager;
import org.pbccrc.zsls.tasktracker.taskhandle.HandleTaskService;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskHandler;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskManager;
import org.pbccrc.zsls.tasktracker.taskreport.TaskReporter;

public class ClientContextImpl implements ClientContext {
	
	private ClientConfig config;
	private RuntimeMeta meta;
	private RegInfo regInfo;
	private TaskReporter reporter;
	private HandleTaskService service;
	private RegisterManager regManager;
	private TaskHandler taskHandler;
	private TaskManager manager;
	
	public ClientContextImpl() {
		regInfo = new RegInfo(RegStatus.Lost, null);
	}

	@Override
	public ClientConfig getConfig() {
		return config;
	}

	@Override
	public void setConfig(ClientConfig config) {
		this.config = config;
	}

	@Override
	public RuntimeMeta getRuntimeMeta() {
		return meta;
	}

	@Override
	public void setRuntimeMeta(RuntimeMeta meta) {
		this.meta = meta;
	}

	@Override
	public RegInfo getRegInfo() {
		return regInfo;
	}

	@Override
	public void setRegInfo(RegInfo regInfo) {
		this.regInfo = regInfo;
	}

	@Override
	public TaskReporter getTaskReporter() {
		return reporter;
	}

	@Override
	public void setTaskReporter(TaskReporter reporter) {
		this.reporter = reporter;
	}

	@Override
	public HandleTaskService getHandleTaskService() {
		return service;
	}

	@Override
	public void setHandleTaskService(HandleTaskService service) {
		this.service = service;
	}

	@Override
	public RegisterManager getRegisterManager() {
		return regManager;
	}

	@Override
	public void setRegisterManager(RegisterManager regManager) {
		this.regManager = regManager;
	}

	@Override
	public TaskHandler getGlobalTaskHandler() {
		return taskHandler;
	}

	@Override
	public void setGlobalTaskHandler(TaskHandler handler) {
		taskHandler = handler;
	}

	@Override
	public TaskManager getTaskManager() {
		return manager;
	}

	@Override
	public void setTaskManager(TaskManager manager) {
		this.manager = manager;
	}

}
