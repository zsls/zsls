package org.pbccrc.zsls.jobengine.statement;

public class ExecuteExp implements ExpObj {
	
	private String type;
	
	private String value;
	
	public ExecuteExp(String type, String value) {
		this.type = type;
		this.value = value;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
