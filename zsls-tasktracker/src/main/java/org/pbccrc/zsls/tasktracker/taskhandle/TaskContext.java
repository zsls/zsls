package org.pbccrc.zsls.tasktracker.taskhandle;

import org.pbccrc.zsls.jobengine.statement.ConditionExp;
import org.pbccrc.zsls.jobengine.statement.ExpParser;

public interface TaskContext {
	
	public static class RetryStrategy {
		public RetryStrategy(String condition, int num) {
			this.condition = ExpParser.parse(condition);
			this.num = num;
		}
		public final ConditionExp condition;
		public final int num;
		public boolean valid() {
			return condition != null && num > 0;
		}
	}
	
	TaskDetail getTaskDetail();
	
	int getLocalRetryTime();
	
	RetryStrategy getRetryStrategy();
	
	ResultWriter getResultWriter();
	
	long getStartTimestamp();
	long getEndTimestamp();
	
}
