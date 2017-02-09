package org.pbccrc.zsls.jobengine.statement;

public class VarExp implements ExpObj {
	
	private String value;
	
	public VarExp(String val) {
		this.value = val;
	}
	
	public String getValue() {
		return value;
	}

}
