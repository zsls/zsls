package org.pbccrc.zsls.tasktracker;

import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.jobengine.statement.Param;
import org.pbccrc.zsls.tasktracker.RegInfo.RegStatus;
import org.pbccrc.zsls.tasktracker.register.RegisterListener;
import org.pbccrc.zsls.tasktracker.register.RegisterResult;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskContext;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskContext.RetryStrategy;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskExecutionInfo;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskFinishCallback;
import org.pbccrc.zsls.tasktracker.taskreport.TaskFinEvent;
import org.pbccrc.zsls.tasktracker.taskreport.TaskFinType;
import org.pbccrc.zsls.tasktracker.taskreport.TaskReportCallback;

public class Controller implements RegisterListener, TaskFinishCallback, 
			ServerLostListener, TaskReportCallback {
	private static Logger L = Logger.getLogger(Controller.class.getSimpleName());
	
	private ClientContext context;
	
	public Controller(ClientContext context) {
		this.context = context;
	}
	
	private boolean changeRegisterStatusToLost() {
		if (context.getRegInfo().getStatus() == RegStatus.Registered) {
			synchronized (this) {
				if (context.getRegInfo().getStatus() == RegStatus.Registered) {
					context.setRegInfo(new RegInfo(RegStatus.Lost, null));
					return true;
				}
			}
		}
		return false;
	}
	
	private boolean changeRegisterStatusToRegistered(InetSocketAddress addr) {
		if (context.getRegInfo().getStatus() == RegStatus.Lost) {
			synchronized (this) {
				if (context.getRegInfo().getStatus() == RegStatus.Lost) {
					context.setRegInfo(new RegInfo(RegStatus.Registered, addr.toString()));
					return true;
				}
			}
		}
		return false;
	}
	
	public void pauseSystemAndReRegister() {
		if (changeRegisterStatusToLost()) {
			context.getTaskReporter().pauseSend();
			RetryRegisterThread register = new RetryRegisterThread(this);
			register.start();	
		}
	}


	@Override
	public void onRegistered(RegisterResult result) {
		if (changeRegisterStatusToRegistered(result.getMasterAddr())) {
			context.setRuntimeMeta(result.getMeta());
			context.getTaskReporter().updateServerAddr(result.getMasterAddr(), result.getMeta());
		}
	}
	
	private class RetryRegisterThread extends Thread {
		RegisterListener listener;
		volatile boolean stop;
		public RetryRegisterThread(RegisterListener listener) {
			this.listener = listener;
		}
		public void run() {
			while (!stop) {
				RegisterResult ret = context.getRegisterManager().tryRegister();
				if (ret != null) {
					listener.onRegistered(ret);
					break;
				}
				try {
					Thread.sleep(3000L);
				} catch (InterruptedException ignore) {
				}
			}
		}
	}
	
	@Override
	public void onTaskComplte(TaskExecutionInfo info) {
		taskFinished(info, true);
	}

	@Override
	public void onTaskFail(TaskExecutionInfo info) {
		taskFinished(info, false);
	}
	
	private void taskFinished(TaskExecutionInfo info, boolean success) {
		TaskContext context = info.context;
		RetryStrategy retryOp = context.getRetryStrategy();
		if (retryOp != null && retryOp.valid()) {
			Param p = Param.getParam(info.keyMessage, success);
			int retryTime = context.getLocalRetryTime();
			String taskId = context.getTaskDetail().getTaskId();
			if (retryTime < retryOp.num && retryOp.condition.getValue(p)) {
				L.info("retry task " + taskId + ", retry time: " + (retryTime + 1));
				this.context.getHandleTaskService().addToRetry(context);
				return;
			}
			else {
				L.info("retry condition not met for task " + taskId + " -> [" + info.keyMessage + ", "
						+ retryTime + "], report task to jobtracker");
			}
		}
		TaskFinType type = success ? TaskFinType.Completed : TaskFinType.Fail;
		TaskFinEvent event = new TaskFinEvent(type, info);
		this.context.getTaskReporter().addToReport(event);
	}

	@Override
	public void onServerLost(InetSocketAddress addr) {
		L.warn("server lost, ready to reregister");
		pauseSystemAndReRegister();
	}

	@Override
	public void onTaskReported(String taskId) {
		context.getTaskManager().unregisterTask(taskId);
	}

}
