package org.pbccrc.zsls.tasktracker.register;

import java.net.InetSocketAddress;

import org.pbccrc.zsls.tasktracker.config.RuntimeMeta;

public class RegisterResult {
	
	private RuntimeMeta meta;
	
	private InetSocketAddress masterAddr;
	
	public RegisterResult(InetSocketAddress masterAddr, RuntimeMeta meta) {
		this.masterAddr = masterAddr;
		this.meta = meta;
	}

	public RuntimeMeta getMeta() {
		return meta;
	}

	public void setMeta(RuntimeMeta meta) {
		this.meta = meta;
	}

	public InetSocketAddress getMasterAddr() {
		return masterAddr;
	}

	public void setMasterAddr(InetSocketAddress masterAddr) {
		this.masterAddr = masterAddr;
	}

}
