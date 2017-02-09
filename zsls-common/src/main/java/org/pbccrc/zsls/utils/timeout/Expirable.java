package org.pbccrc.zsls.utils.timeout;

public interface Expirable {
	
	long expireTime();
	
	boolean timeoutCanceled();
	
	String getUniqueId();
	
}
