package org.pbccrc.zsls.api.client.old;

import org.pbccrc.zsls.api.client.old.SchedResult.RetCode;

public class InnerSchedResult {
	
	public static enum ServerStat {
		READY,
		STANDBY,
		UNKNOWN,
	}
	public String masterAddress;
	public String generatedId;
	public ServerStat serverStat;
	public RetCode retCode;
	
	public String info;
	
	public String toString() {
		return "retCode: " + retCode.name() + " , id: " + generatedId + ", info: " + info;
	}
}
