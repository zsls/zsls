package org.pbccrc.zsls.store.jdbc;

import javax.sql.DataSource;

import org.pbccrc.zsls.config.DbConfig;

/**
 * @author Robert HG (254963746@qq.com) on 10/24/14.
 */
public interface DataSourceProvider {

    DataSource getDataSource(DbConfig config) throws Exception;

}
