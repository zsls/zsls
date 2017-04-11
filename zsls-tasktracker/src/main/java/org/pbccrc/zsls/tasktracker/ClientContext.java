package org.pbccrc.zsls.tasktracker;

import org.pbccrc.zsls.tasktracker.config.ClientConfig;
import org.pbccrc.zsls.tasktracker.config.RuntimeMeta;
import org.pbccrc.zsls.tasktracker.register.RegisterManager;
import org.pbccrc.zsls.tasktracker.taskhandle.HandleTaskService;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskHandler;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskManager;
import org.pbccrc.zsls.tasktracker.taskreport.TaskReporter;

public interface ClientContext {
	
	ClientConfig getConfig();
	void setConfig(ClientConfig config);
	
	RuntimeMeta getRuntimeMeta();
	void setRuntimeMeta(RuntimeMeta meta);
	
	RegInfo getRegInfo();
	void setRegInfo(RegInfo regInfo);
	
	TaskReporter getTaskReporter();
	void setTaskReporter(TaskReporter reporter);
	
	HandleTaskService getHandleTaskService();
	void setHandleTaskService(HandleTaskService service);
	
	RegisterManager getRegisterManager();
	void setRegisterManager(RegisterManager regManager);
	
	TaskHandler getGlobalTaskHandler();
	void setGlobalTaskHandler(TaskHandler handler);
	
	TaskManager getTaskManager();
	void setTaskManager(TaskManager manager);
	
}
