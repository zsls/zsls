package org.pbccrc.zsls.api.quartz;

import org.quartz.Trigger;

public abstract class QuartzTrigger {
	public static final String KEY_DOMAIN		= "domain";
	public static final String KEY_JOBFLOW	= "jobFlow";
	public static final String GROUP_ZSLS		= "zsls";
	protected static TriggerType triggerType;
	
	public abstract TriggerType getTriggerType();
	
	public static enum TriggerType {
		SIMPLE(0),
		CRONTAB(1);
		
		private int value;
		
		TriggerType(int value) {
			this.value = value;
		}
		
		public int getValue() {
			return this.value;
		}
		
		public static TriggerType getInstance(int value) {
			for (TriggerType t: TriggerType.values()) {
				if (t.value == value)
					return t;
			}
			return null;
		}
	}
	
	public abstract Trigger getTrigger(String id);
	
}
