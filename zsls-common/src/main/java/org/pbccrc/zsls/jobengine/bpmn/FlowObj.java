package org.pbccrc.zsls.jobengine.bpmn;

import java.util.List;

import org.pbccrc.zsls.jobengine.JobFlow;

public interface FlowObj {
	
	List<DataFlow> getInFlows();
	
	List<DataFlow> getOutFlows();
	
	JobFlow getJobFlow();
	
	void setJobFlow(JobFlow job);

}
