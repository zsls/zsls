package org.pbccrc.zsls.tasktracker.taskhandle.handler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.tasktracker.ClientContext;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskContext;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskDetail;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskExecutionInfo;
import org.pbccrc.zsls.tasktracker.taskhandle.TaskHandler;
import org.pbccrc.zsls.utils.FileUtils;
import org.pbccrc.zsls.utils.Shell;
import org.pbccrc.zsls.utils.Shell.KeyShellCommandExecutor;
import org.pbccrc.zsls.utils.Shell.ShellCommandExecutor;

import com.google.gson.Gson;

/**
 * File system structure to store task scripts and PID files. 
 * "idx" is the task id of the launching task.
 * 
 *						|-- id1.pid
 * 			|--- id1 ---|
 * 			|			|-- start_id1.sh
 * runtime--
 * 			|			|-- id2.pid
 * 			|--- id2 ---| 
 * 						|-- start_id2.sh
 * 
 * Two must-have task parameters for shell tasks:
 * 		<code>ZslsConstants.TASKP_SHELL_DIR</code> and <code>ZslsConstants.TASKP_SHELL_SCRIPT</code>.
 * other parameters will be expanded as the script's input parameters. 
 * for example, for task configured with parameters below:
 * 		<params>
 * 			<entry>
 * 				<string>cmd.dir</string><string>/usr/task</string>
 * 				<string>cmd.script</string><string>start.sh</string>
 * 				<string>key1</string><string>val1</string>
 * 				<string>key2</string><string>val2</string>
 * 			</entry>
 * 		</params>
 * 
 * the final execute script would be:
 * 		"cd /usr/task && start.sh key1 val1 key2 val2"
 * but note that key1 key2 val1 val2 are all url_encoded in case they have blank spaces
 */
public class ShellTaskExecutor implements TaskHandler {
	public static final String APPEND_PID		= ".pid";
	public static final String APPEND_SH		= ".sh";
	public static final String APPEND_CMD     = ".cmd";
	public static final String DIR_RUN		= "runtime";
	public static final String PREF_SCRIPT	= "start_";
	
	private static Logger L = Logger.getLogger(ShellTaskExecutor.class.getSimpleName());
	
	/**
	 * ExitCode constants
	 */
	public enum ExitCode {
		FORCE_KILLED(137), TERMINATED(143), LOST(154);
		
		private final int code;
		private ExitCode(int exitCode) {
			this.code = exitCode;
		}
		public int getExitCode() {
			return code;
		}
		@Override
		public String toString() {
			return String.valueOf(code);
		}
	}

	/**
	 * The constants for the signals.
	 */
	public enum Signal {
		NULL(0, "NULL"), QUIT(3, "SIGQUIT"), 
		KILL(9, "SIGKILL"), TERM(15, "SIGTERM");
		
		private final int value;
		private final String str;
		private Signal(int value, String str) {
			this.str = str;
			this.value = value;
		}
		public int getValue() {
			return value;
		}
		@Override
		public String toString() {
			return str;
		}
	}

	@Override
	public boolean init() {
		return true;
	}
	
	private void logAndSendFeedback(TaskContext context, String msg) {
		context.getResultWriter().writeFeedbackMessage(msg);
		L.error(msg);
	}
	
	private ClientContext context;
	public void setClientContext(ClientContext context) {
		this.context = context;
	}

	@Override
	public boolean handleTask(TaskContext context) {
		TaskDetail task = context.getTaskDetail();
		String taskId = task.getTaskId();
		String dir = task.getParams().get(ZslsConstants.TASKP_SHELL_DIR);
		String script = task.getParams().get(ZslsConstants.TASKP_SHELL_SCRPT);
		if (dir == null || script == null) {
			logAndSendFeedback(context, "Null directory or script name");
			return false;
		}
		ShellCommandExecutor exc = null;
		try {
//			List<String> params = expandParams(task.getParams());
			exc = buildCommandExecutor(dir, script, taskId, task.getParams());
		} catch (Exception e) {
			e.printStackTrace();
			logAndSendFeedback(context, "failed to build command executor, " + e.getMessage());
			return false;
		}
		try {
			TaskExecutionInfo info = this.context.getTaskManager().getTaskExecutionInfo(taskId);
			info.shell = exc;
			exc.execute();
			context.getResultWriter().writeKeyMessage(exc.getKeyMessage());
			context.getResultWriter().updateRuntimeParams(parseRuntimeParam(exc.getRuntimeMeta()));
			return true;
		} catch (IOException e) {
			int exitCode = exc.getExitCode();
			if (exitCode == 0) {
				L.warn("process of task " + taskId + " not started");
				e.printStackTrace();
			}
			else if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
					&& exitCode != ExitCode.TERMINATED.getExitCode()) {
				L.error("task failed, exitCode: " + exitCode);
			} else {
				L.warn("task " + taskId + " killed by force, exitCode: " + exitCode);
			}
			e.printStackTrace();
		} finally {
			if (exc != null)
				exc.close();
			try {
				cleanTaskPath(taskId);
			} catch (Exception ignore) {
				L.error("failed to clean task path: " + taskId);
			}
		}
		return false;
	}
	
	@SuppressWarnings("unchecked")
	private static Map<String, String> parseRuntimeParam(String in) {
		if (in == null || in.isEmpty())
			return null;
		Map<String, String> ret = null;
		try {
			ret = new Gson().fromJson(in, Map.class);
		} catch (Exception e) {
			L.error("invalid runtime param: " + in);
		}
		return ret;
	}
	
	// transfer params as main args
	/*private List<String> expandParams(Map<String, String> p) throws UnsupportedEncodingException {
		List<String> list = new ArrayList<String>();
		for (String k : p.keySet()) {
			if (ZslsConstants.TASKP_SHELL_DIR.equals(k) || ZslsConstants.TASKP_SHELL_SCRPT.equals(k))
				continue;
			String v = p.get(k);
			list.add(URLEncoder.encode(k, "UTF-8"));
			list.add(URLEncoder.encode(v, "UTF-8"));
		}
		return list;
	}*/
	// transfer params as jvm args
	protected List<String> expandParams(Map<String, String> p) throws UnsupportedEncodingException {
		List<String> list = new ArrayList<String>();
		for (String k : p.keySet()) {
			if (ZslsConstants.TASKP_SHELL_DIR.equals(k) || ZslsConstants.TASKP_SHELL_SCRPT.equals(k))
				continue;
			String v = p.get(k);
			String jvmParam = "-D" + k + "=" + URLEncoder.encode(v, "UTF-8");
			list.add(jvmParam);
		}
		return list;
	}
	
	protected ShellCommandExecutor buildCommandExecutor(String dir, String script, 
			String taskId, Map<String, String> params) throws IOException {
		String[] command = null;
		if (Shell.LINUX)
			command = makeScript(dir, script, taskId, params);
		else if (Shell.WINDOWS)
			command = makeCMD(dir, script, taskId, params);
		L.info("launch command: " + Arrays.toString(command));
		return new KeyShellCommandExecutor(command);
	}
	
	protected String[] makeCMD(String dir, String script, String taskId, Map<String, String> params) throws IOException {
		String separator = File.separator;
		String path = DIR_RUN + separator + taskId;
		//absolute path needed
		path = FileUtils.createDirIfNotExist(path).getAbsolutePath();
		String scriptPath = path + separator + PREF_SCRIPT + taskId + APPEND_CMD;
		BufferedWriter writer = makeFileWriter(scriptPath);
		if (dir.endsWith(separator)) 
			dir.substring(0, dir.length() - 1);
		if (script.startsWith(separator))
			script.substring(1);
		try {
			writer.write("@echo off\n");
			//not work with processbuilder.start 
			/*String pidfile = taskId + APPEND_PID;
			writer.write("cd " + path + "\n");
			writer.write("title=" + taskId + "\n");
			writer.write("for /f \"tokens=2\" %%a in ('tasklist /v /nh /fi \"windowtitle eq "+ taskId + "\"') do echo %%a > \"" + pidfile + ".tmp\"\n");
			writer.write("move " + pidfile + ".tmp " + pidfile + "\n");	*/
			for (String k : params.keySet()) {
				String v = params.get(k);
				writer.write("set " + k + " = " + v + "\n");
			}
			writer.write("cd " + dir + "\n");
			writer.write("cmd /c " + script + "\n");
			/*for (String s : params)
				writer.write(" " + s);*/
			writer.flush();
		} finally {
			writer.close();
		}
		return new String[]{"cmd", "/c", scriptPath};
	}
	
	protected String[] makeScript(String dir, String script, String taskId, Map<String, String> params) 
			throws IOException {
		String separator = File.separator;
		String path = DIR_RUN + separator + taskId;
		FileUtils.createDirIfNotExist(path);
		String scriptPath = path + separator + PREF_SCRIPT + taskId + APPEND_SH;
		BufferedWriter writer = makeFileWriter(scriptPath);
		try {
			writer.write("#!/bin/bash\n");
			String pidfile = path + separator + taskId + APPEND_PID;
			writer.write("echo $$ > " + pidfile + ".tmp\n");	
			writer.write("mv " + pidfile + ".tmp " + pidfile + "\n");
			for (String k : params.keySet()) {
				String v = params.get(k);
				writer.write("export " + k + "=" + v + "\n");
			}
			writer.write("cd " + dir + " && sh " + script);
			writer.write("\n");
			writer.flush();
		} finally {
			writer.close();
		}
		return new String[]{"sh", scriptPath};
	}
	
	private BufferedWriter makeFileWriter(String path) throws IOException {
		File file = FileUtils.createFileIfNotExist(path);
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		return writer;
	}
	
	public static void cleanTaskPath(String taskId) {
		String path = DIR_RUN + "/" + taskId;
		File file = new File(path);
		if (file.exists()) {
			File tmp = new File(path + ".tmp");
			file.renameTo(tmp);
			FileUtils.delete(tmp);
		}
	}

}
