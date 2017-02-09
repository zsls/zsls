package org.pbccrc.zsls.front;

public interface ResultSerializable {
	public static final int FLAG_		= 0;
	
	// fast speed
	boolean serialize(StringBuilder builder, int flag);
	
	// low speed, yet easy to use. 
	// returns object that can be serialized to JSON object
	Object serialize(int flag);

}