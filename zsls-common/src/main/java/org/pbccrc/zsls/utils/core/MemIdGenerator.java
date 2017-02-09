package org.pbccrc.zsls.utils.core;

public class MemIdGenerator extends AbstractIdGenerator {

	public MemIdGenerator() throws Exception {
		super();
	}

	@Override
	public void updateLimit(long limit) throws Exception {
		// do nothing
	}

	@Override
	public long fetchLimit() throws Exception {
		return 0;
	}

}
