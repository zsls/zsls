package org.pbccrc.zsls.jobstore.oracle;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.DbConfig;
import org.pbccrc.zsls.jobstore.JobStore;
import org.pbccrc.zsls.jobstore.JobStoreFactory;

public class OracleJobStoreFactory implements JobStoreFactory {

	@Override
	public JobStore getJobStore(Configuration config) {
		DbConfig dbConfig = DbConfig.readConfig(config);
		JobStore mysqlJobStore = new OracleJobStore(dbConfig);
		return mysqlJobStore;
	}

}
