package org.pbccrc.zsls.store.jdbc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.pbccrc.zsls.config.DbConfig;

/**
 *
 */
public class SqlTemplateFactory {

    private static final ConcurrentMap<DataSource, SqlTemplate> HOLDER = new ConcurrentHashMap<DataSource, SqlTemplate>();

    public static SqlTemplate create(DbConfig config) {
        DataSourceProvider dataSourceProvider = new DruidDataSourceProvider();
        DataSource dataSource;
		try {
			dataSource = dataSourceProvider.getDataSource(config);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
        SqlTemplate sqlTemplate = HOLDER.get(dataSource);

        if (sqlTemplate != null) {
            return sqlTemplate;
        }
        synchronized (HOLDER) {
            sqlTemplate = HOLDER.get(dataSource);
            if (sqlTemplate != null) {
                return sqlTemplate;
            }
            sqlTemplate = new SqlTemplateImpl(dataSource);
            HOLDER.putIfAbsent(dataSource, sqlTemplate);
            return sqlTemplate;
        }
    }

}
