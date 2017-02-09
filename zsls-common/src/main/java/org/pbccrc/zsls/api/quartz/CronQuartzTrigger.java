package org.pbccrc.zsls.api.quartz;

import org.quartz.CronScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

public class CronQuartzTrigger extends QuartzTrigger {
	
	private String cronExpression;
	

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	@Override
	public Trigger getTrigger(String id) {
		return TriggerBuilder.newTrigger()
					.withIdentity(id, GROUP_ZSLS)
					.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
					.build();
	}

	@Override
	public TriggerType getTriggerType() {
		return TriggerType.CRONTAB;
	}

}
