package org.pbccrc.zsls.tasktracker.config;

import org.pbccrc.zsls.utils.ThreadLocalBuffer;

public class ClientConfig {
	
	private String serverAddrs;
	
	private int iothreads;
	
	private String domain;
	
	private boolean dtDomain;
	
	private String name;	// optional
	
	private int recvPort;
	
	private int maxTaskNum;
	
	private String taskHandler;
	
	
	public String getTaskHandler() {
		return taskHandler;
	}

	public void setTaskHandler(String taskHandler) {
		this.taskHandler = taskHandler;
	}

	public int getIothreads() {
		return iothreads;
	}

	public void setIothreads(int iothreads) {
		this.iothreads = iothreads;
	}

	public String getServerAddrs() {
		return serverAddrs;
	}

	public void setServerAddrs(String serverAddrs) {
		this.serverAddrs = serverAddrs;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getRecvPort() {
		return recvPort;
	}

	public void setRecvPort(int recvPort) {
		this.recvPort = recvPort;
	}

	public int getMaxTaskNum() {
		return maxTaskNum;
	}

	public void setMaxTaskNum(int maxTaskNum) {
		this.maxTaskNum = maxTaskNum;
	}
	
	public boolean isDtDomain() {
		return dtDomain;
	}

	public void setDtDomain(boolean isDtDomain) {
		this.dtDomain = isDtDomain;
	}

	public String toString() {
		StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
		b.append("[");
		b.append("domain -> ").append(domain).append(", ");
		b.append("isDtDomain -> ").append(dtDomain).append(", ");
		b.append("receivePort -> ").append(recvPort).append(", ");
		b.append("ioThreads -> ").append(iothreads).append(", ");
		b.append("name -> ").append(name).append(", ");
		b.append("maxTasks -> ").append(maxTaskNum).append(", ");
		b.append("taskHandler -> ").append(taskHandler);
		b.append("]");
		return b.toString();
	}

}
