package org.pbccrc.zsls.jobstore.mysql;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.DbConfig;
import org.pbccrc.zsls.jobstore.JobStore;
import org.pbccrc.zsls.jobstore.JobStoreFactory;

public class MysqlJobStoreFactory implements JobStoreFactory {

	@Override
	public JobStore getJobStore(Configuration config) {
		DbConfig dbConfig = DbConfig.readConfig(config);
		JobStore mysqlJobStore = new MysqlJobStore(dbConfig);
		return mysqlJobStore;
	}

}
