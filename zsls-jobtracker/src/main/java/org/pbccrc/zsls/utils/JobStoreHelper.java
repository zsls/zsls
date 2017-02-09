package org.pbccrc.zsls.utils;

import org.pbccrc.zsls.jobstore.JobStoreFactory;
import org.pbccrc.zsls.jobstore.mysql.MysqlJobStoreFactory;
import org.pbccrc.zsls.jobstore.oracle.OracleJobStoreFactory;
import org.pbccrc.zsls.jobstore.zookeeper.ZookeeperJobStoreFactory;

public class JobStoreHelper {
	public static final String MYSQL		= "mysql";
	public static final String ORACLE		= "oracle";
	public static final String ZOOKEEPER	= "zookeeper";
	
	public static JobStoreFactory getJobStoreFactory(String url) {
		if (url == null)
			throw new IllegalArgumentException("invalid job store url");
		if (url.contains(MYSQL))
			return new MysqlJobStoreFactory();
		else if (url.contains(ORACLE))
			return new OracleJobStoreFactory();
		else if (url.contains(ZOOKEEPER))
			return new ZookeeperJobStoreFactory();
		
		return new OracleJobStoreFactory();
	}

}
