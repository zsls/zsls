package org.pbccrc.zsls.eventdispatch;

import org.pbccrc.zsls.entry.Domained;

public class AbstractDomainedEvent<TYPE extends Enum<TYPE>> extends 
		AbstractEvent<TYPE> implements Domained {
	
	public AbstractDomainedEvent(TYPE type) {
		super(type);
		throw new IllegalStateException("domain must be defined for AbstractDomainedEvent");
	}
	
	public AbstractDomainedEvent(TYPE type, String domain) {
		super(type);
		this.domain = domain;
	}

	private String domain;

	@Override
	public String getDomain() {
		return domain;
	}

}
