package org.pbccrc.zsls.jobengine;

public interface PriorityQueuable extends Comparable<PriorityQueuable> {
	
	public static final int MAX_PRIORITY 		= 10;
	public static final int MIN_PRIORITY 		= 0;
	public static final int DEFAULT_PRIORITY 	= 0;
	
	double getPriority();

}