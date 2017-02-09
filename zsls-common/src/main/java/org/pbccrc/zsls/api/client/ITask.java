package org.pbccrc.zsls.api.client;

import java.util.Map;

public class ITask {
	
	public String id;
	
	public String domain;
	
	public String targetNode;
	
	public long timeout;
	
	public Map<String, String> params;
	
	public IRetryStrategy retryOp;
	
	public int partitions = 1;

}
