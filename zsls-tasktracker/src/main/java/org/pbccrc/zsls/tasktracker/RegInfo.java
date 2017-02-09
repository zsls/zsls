package org.pbccrc.zsls.tasktracker;

public class RegInfo {
	
	public static enum RegStatus {
		Registered,
		Lost
	}
	
	private RegStatus status;
	
	private String masterAddr;
	
	public RegInfo(RegStatus status, String masterAddr) {
		this.status = status;
		this.masterAddr = masterAddr;
	}

	public RegStatus getStatus() {
		return status;
	}

	public void setStatus(RegStatus status) {
		this.status = status;
	}

	public String getMasterAddr() {
		return masterAddr;
	}

	public void setMasterAddr(String masterAddr) {
		this.masterAddr = masterAddr;
	}

}
