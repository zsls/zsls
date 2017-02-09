package org.pbccrc.zsls.api.client.old;

import java.util.ArrayList;
import java.util.List;

public class IReEntry {
	
	public String id;
	
	public List<IUserTask> tasks;
	
	public IReEntry() {
		
	}
	public IReEntry(String id) {
		this.id = id;
		tasks = new ArrayList<IUserTask>();
	}

}
