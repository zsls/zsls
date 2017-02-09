package org.pbccrc.zsls.api.quartz.builder;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.pbccrc.zsls.api.quartz.SimpleQuartzTrigger;

/**
 *	like quartz, endTime prior to repeats
 */
public class SimpleTriggerBuilder extends TriggerBuilder {
	
	private Date startTime;
	
	private Date endTime;
	
	private long interval;
	
	private long repeats;
	
	public void buildCheck() throws IllegalArgumentException {
		super.buildCheck();
		if (repeats > 0 && interval == 0L)
			throw new IllegalArgumentException("interval of 0 not accepted if repeats multiple times");
		
		if (startTime == null)
			startTime = new Date();
	}
	
	public SimpleQuartzTrigger build() {
		buildCheck();
		SimpleQuartzTrigger trigger = new SimpleQuartzTrigger();
		trigger.setInterval(interval);
		trigger.setEndTime(endTime);
		trigger.setRepeats(repeats);
		trigger.setStartTime(startTime);
		return trigger;
	}
	
	public SimpleTriggerBuilder startNow() {
		startTime = new Date();
		return this;
	}
	
	public SimpleTriggerBuilder startAt(Date start) {
		this.startTime = start;
		return this;
	}
	
	public SimpleTriggerBuilder endTime(Date end) {
		this.endTime = end;
		return this;
	}
	
	public SimpleTriggerBuilder withInterval(long interval, TimeUnit unit) {
		this.interval = TimeUnit.SECONDS.convert(interval, unit);
		return this;
	}
	
	public SimpleTriggerBuilder repeats(long repeats) {
		this.repeats = repeats;
		return this;
	}
	
	public SimpleTriggerBuilder jobData(String key, String val) {
		this.data.put(key, val);
		return this;
	}

}
