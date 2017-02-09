package org.pbccrc.zsls.api.quartz;

import java.util.Date;

import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

/**
 *	like quartz, endTime prior to repeats
 */
public class SimpleQuartzTrigger extends QuartzTrigger {
	
	private Date startTime;
	
	private Date endTime;
	
	private long interval;	// in second
	
	private long repeats;
	
	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public long getInterval() {
		return interval;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}

	public long getRepeats() {
		return repeats;
	}

	public void setRepeats(long repeats) {
		this.repeats = repeats;
	}

	@Override
	public Trigger getTrigger(String id) {
		TriggerBuilder<Trigger> b = TriggerBuilder.newTrigger().withIdentity(id, GROUP_ZSLS);
		if (startTime != null)
			b.startAt(startTime);
		if (endTime != null)
			b.endAt(endTime);

		if (getInterval() > 0 || getRepeats() > 1) {
			SimpleScheduleBuilder sb = SimpleScheduleBuilder.simpleSchedule()
					.withIntervalInSeconds((int)interval);
			if (repeats > 0)
				sb.withRepeatCount((int)repeats);
			b.withSchedule(sb);
		}

		return b.build();
	}

	@Override
	public TriggerType getTriggerType() {
		return TriggerType.SIMPLE;
	}
	
}
