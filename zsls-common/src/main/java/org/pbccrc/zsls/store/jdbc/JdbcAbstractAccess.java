package org.pbccrc.zsls.store.jdbc;

import java.io.IOException;
import java.io.InputStream;

import org.pbccrc.zsls.config.DbConfig;
import org.pbccrc.zsls.exception.ZslsRuntimeException;
import org.pbccrc.zsls.exception.store.JdbcException;
import org.pbccrc.zsls.utils.FileUtils;

/**
 * @author Robert HG (254963746@qq.com) on 5/19/15.
 */
public abstract class JdbcAbstractAccess {

    private SqlTemplate sqlTemplate;

    public JdbcAbstractAccess(DbConfig config) {
        this.sqlTemplate = SqlTemplateFactory.create(config);
    }

    public SqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }

    protected String readSqlFile(String path) {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(path);
        try {
            return FileUtils.read(is, "utf-8");
        } catch (IOException e) {
            throw new ZslsRuntimeException("Read sql file : [" + path + "] error ", e);
        }
    }

    protected String readSqlFile(String path, String tableName) {
        String sql = readSqlFile(path);
        return sql.replace("{tableName}", tableName);
    }

    protected void createTable(String sql) throws JdbcException {
        try {
            getSqlTemplate().createTable(sql);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new JdbcException("Create table error, sql=" + sql, e);
        }
    }
}
