package org.pbccrc.zsls.front.result;

import java.util.LinkedList;
import java.util.List;

public class ResultLeafList extends Result {
	
	private List<Object> data;
	
	public ResultLeafList() {
		data = new LinkedList<Object>();
	}
	
	public void addElement(Object o) {
		data.add(o);
	}
	
	public List<Object> getElements() {
		return data;
	}

	@Override
	public Object data() {
		return data;
	}

}
