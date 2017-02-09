package org.pbccrc.zsls.utils.timeout;

public class RealtimeExpireItem implements Expirable {

	private long expireTime;
	public RealtimeExpireItem(long expireTime) {
		this.expireTime = System.currentTimeMillis() + expireTime;
	}
	
	@Override
	public long expireTime() {
		return expireTime;
	}

	@Override
	public boolean timeoutCanceled() {
		return false;
	}
	
	@Override
	public String getUniqueId() {
		return "";
	}

}
