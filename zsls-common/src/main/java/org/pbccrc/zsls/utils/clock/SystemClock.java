package org.pbccrc.zsls.utils.clock;

public class SystemClock implements Clock {
	
	@Override
	public long getTime() {
		return System.currentTimeMillis();
	}

}
