package org.pbccrc.zsls.api.client;

import java.util.List;

public class IJobFlow {
	
	public String id;
	
	public boolean refreshIfPrevJobStucked;
	
	public List<IDataFlow> dataFlows;
	
	public List<Object> flowObjs;

}
