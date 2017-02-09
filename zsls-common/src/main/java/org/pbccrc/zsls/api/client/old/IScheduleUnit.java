package org.pbccrc.zsls.api.client.old;

import java.util.ArrayList;
import java.util.List;

public class IScheduleUnit {
	
	public String domain;
	
	public long timeout;
	
	public List<IRelation> relations;
	
	public List<IUserTask> independentTasks;
	
	public String preUnit;
	
	public String swiftNum;
	
	public IScheduleUnit() {
	}
	
	public IScheduleUnit(String domain) {
		this.domain = domain;
		relations = new ArrayList<IRelation>();
		independentTasks = new ArrayList<IUserTask>();
	}

}
