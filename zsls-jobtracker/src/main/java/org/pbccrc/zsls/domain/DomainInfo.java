package org.pbccrc.zsls.domain;

import org.pbccrc.zsls.JobTracker.DomainStatus;
import org.pbccrc.zsls.domain.DomainManager.DomainType;

public class DomainInfo {
	
	public String name;
	
	public DomainType type;
	
	// 暂时不用，域状态以DomainManager里边的状态机为准。
	public DomainStatus status;
	
	public DomainInfo(String name, DomainType type) {
		this.name = name;
		this.type = type;
		this.status = DomainStatus.Init;
	}

}
