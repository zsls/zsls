package org.pbccrc.zsls.jobengine.bpmn;

import java.util.LinkedList;
import java.util.List;

import org.pbccrc.zsls.jobengine.JobFlow;

public class FlowObjImpl implements FlowObj {

	protected List<DataFlow> in = new LinkedList<DataFlow>();
	
	protected List<DataFlow> out = new LinkedList<DataFlow>();
	
	protected JobFlow job;
	
	public void addInFlow(DataFlow obj) {
		in.add(obj);
	}
	
	public void addOutFlow(DataFlow obj) {
		out.add(obj);
	}
	
	@Override
	public List<DataFlow> getInFlows() {
		return in;
	}

	@Override
	public List<DataFlow> getOutFlows() {
		return out;
	}

	@Override
	public JobFlow getJobFlow() {
		return job;
	}

	@Override
	public void setJobFlow(JobFlow job) {
		this.job = job;
	}

}
