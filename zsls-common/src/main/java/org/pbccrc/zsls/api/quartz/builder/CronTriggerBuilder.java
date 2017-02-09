package org.pbccrc.zsls.api.quartz.builder;

import org.pbccrc.zsls.api.quartz.CronQuartzTrigger;

public class CronTriggerBuilder extends TriggerBuilder {
	
	private String cronExpression;
	
	public CronQuartzTrigger build() {
		super.buildCheck();
		if (cronExpression == null)
			throw new IllegalArgumentException("cronExpression not assigned");
		CronQuartzTrigger trigger = new CronQuartzTrigger();
		trigger.setCronExpression(cronExpression);
		return trigger;
	}
	
	public CronTriggerBuilder withExpression(String expression) {
		this.cronExpression = expression;
		return this;
	}
	
	public CronTriggerBuilder jobData(String key, String val) {
		this.data.put(key, val);
		return this;
	}

}
