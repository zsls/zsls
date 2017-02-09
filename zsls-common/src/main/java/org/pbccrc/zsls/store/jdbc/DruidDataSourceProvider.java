package org.pbccrc.zsls.store.jdbc;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.pbccrc.zsls.config.DbConfig;

import com.alibaba.druid.pool.DruidDataSourceFactory;

public class DruidDataSourceProvider implements DataSourceProvider {

	@Override
	public DataSource getDataSource(DbConfig config) throws Exception {
		DataSource ds = DruidDataSourceFactory.createDataSource(genProperty(config));
		return ds;
	}
	
	private Map<String, String> genProperty(DbConfig config) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("url", config.getUrl());
		map.put("username", config.getUser());
		map.put("password", config.getPwd());
		map.put("initialSize", String.valueOf(config.getInitialSize()));
		map.put("minIdle", String.valueOf(config.getMinIdle()));
		map.put("maxActive", String.valueOf(config.getMaxActive()));
		map.put("maxWait", String.valueOf(config.getMaxWait()));
		return map;
	}

}
