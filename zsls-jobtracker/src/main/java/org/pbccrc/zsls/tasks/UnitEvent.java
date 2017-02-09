package org.pbccrc.zsls.tasks;

import org.pbccrc.zsls.eventdispatch.AbstractEvent;
import org.pbccrc.zsls.jobengine.JobId;

public class UnitEvent extends AbstractEvent<UnitEventType> {
	
	private JobId unitId;
	
	private String domain;
	
	public UnitEvent(String domain, JobId id, UnitEventType type) {
		super(type);
		this.unitId = id;
		this.domain = domain;
	}
	
	public JobId getUnitId() {
		return unitId;
	}
	
	public String getDomain() {
		return domain;
	}

}
