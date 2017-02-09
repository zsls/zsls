package org.pbccrc.zsls.jobstore.zookeeper;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.jobstore.JobStore;
import org.pbccrc.zsls.jobstore.JobStoreFactory;

public class ZookeeperJobStoreFactory implements JobStoreFactory {

	@Override
	public JobStore getJobStore(Configuration config) {
		return new ZookeeperJobStore();
	}

}
