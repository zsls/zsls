package org.pbccrc.zsls.jobengine.bpmn;

import org.pbccrc.zsls.jobengine.statement.ConditionExp;
import org.pbccrc.zsls.jobengine.statement.Param;

public class DataFlow {

	private FlowObj source;

	private FlowObj target;

	private ConditionExp condition;
	
	// only active data flows are engaged in judging converge-gateway.
	// default true
	private boolean active;
	
	// whether the data flow has been passed through or not.
	// default false
	private boolean flowed;
	
	public DataFlow(FlowObj src, FlowObj target, ConditionExp condition) {
		this.source = src;
		this.target = target;
		this.condition = condition;
		this.active = true;
	}
	
	public void takeEffect() {
		source.getOutFlows().add(this);
		target.getInFlows().add(this);
	}

	public FlowObj getSource() {
		return source;
	}

	public FlowObj getTarget() {
		return target;
	}

	public ConditionExp getCondition() {
		return condition;
	}
	
	public boolean isConditionMet(Param param) {
		return condition == null || condition.getValue(param);
	}
	
	public void setActive(boolean active) {
		this.active = active;
	}
	
	public boolean isActive() {
		return active;
	}

	public boolean isFlowed() {
		return flowed;
	}

	public void setFlowed(boolean flowed) {
		this.flowed = flowed;
	}

}
