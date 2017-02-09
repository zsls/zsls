package org.pbccrc.zsls.api.client;

public class IRetryStrategy {
	
	public IRetryStrategy(String condition, int num) {
		this.condition = condition;
		this.num = num;
	}
	
	public boolean valid() {
		return condition != null && num > 0;
	}
	
	public String condition;
	
	public int num;
	
	public String toString() {
		return condition + ";" + String.valueOf(num);
	}

}
