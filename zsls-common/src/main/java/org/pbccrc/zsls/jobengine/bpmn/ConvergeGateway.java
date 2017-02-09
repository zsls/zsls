package org.pbccrc.zsls.jobengine.bpmn;

public class ConvergeGateway extends GateWay {
	
	private boolean selective;
	
	public ConvergeGateway(boolean selective) {
		this.selective = selective;
	}
	
	public boolean converge(DataFlow flow) {
		if (!this.in.contains(flow))
			return false;
		flow.setFlowed(true);
		if (selective) {
			for (DataFlow f : in) {
				if (f.isActive() && !f.isFlowed())
					return false;
			}
		}
		else {
			for (DataFlow f : in) {
				if (!f.isFlowed())
					return false;
			}
		}
		return true;
	}

}
