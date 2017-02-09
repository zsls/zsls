package org.pbccrc.zsls.jobstore;

import org.pbccrc.zsls.config.Configuration;

public interface JobStoreFactory {
	
	JobStore getJobStore(Configuration config);
	
}
