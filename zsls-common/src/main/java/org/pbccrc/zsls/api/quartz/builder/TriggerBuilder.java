package org.pbccrc.zsls.api.quartz.builder;

import java.util.HashMap;
import java.util.Map;

public abstract class TriggerBuilder {
	
	protected Map<String, String> data = new HashMap<String, String>();
	
	protected void buildCheck() throws IllegalArgumentException {
		
	}
	
	public static SimpleTriggerBuilder newSimpleBuilder() {
		return new SimpleTriggerBuilder();
	}
	
	public static CronTriggerBuilder newCronBuilder() {
		return new CronTriggerBuilder();
	}

}
