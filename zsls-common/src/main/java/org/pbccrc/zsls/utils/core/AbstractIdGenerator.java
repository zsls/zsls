package org.pbccrc.zsls.utils.core;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 自增ID生成器.
 * 通过持久化，可以保证即便每次生成器重启，产生的ID仍然比重启之前大。
 */
public abstract class AbstractIdGenerator {
	
	public static long DEFAULT_STEP = 200L;
	
	protected long step;
	
	protected volatile long currentLimit;
	
	protected AtomicLong currentId;
	
	public AbstractIdGenerator() throws Exception {
		this(DEFAULT_STEP);
	}
	
	public AbstractIdGenerator(long step) throws Exception {
		this.step = step;
		long currentLimit = fetchLimit();
		currentId = new AtomicLong(currentLimit);
		currentLimit = currentLimit + step;
	}
	
	public long generateId() throws Exception {
		long id = currentId.getAndIncrement();
		if (id >= currentLimit) {
			synchronized (this) {
				if (id >= currentLimit) {
					currentLimit += step;
					updateLimit(currentLimit);			
				}
			}
		}
		return id;
	}
	
	// 持久化上限值
	abstract public void updateLimit(long limit) throws Exception;
	
	// 获取当前上限值
	abstract public long fetchLimit() throws Exception;

}
