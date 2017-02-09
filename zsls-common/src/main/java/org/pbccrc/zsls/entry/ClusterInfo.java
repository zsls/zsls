package org.pbccrc.zsls.entry;

import java.util.List;

public class ClusterInfo {
	
	String masterAddr;
	
	List<String> cluster;
	
	public ClusterInfo(String masterAddr) {
		this.masterAddr = masterAddr;
	}
	
	public String getMasterAddr() {
		return masterAddr;
	}

}
