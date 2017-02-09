package org.pbccrc.zsls.store.jdbc.builder;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.exception.store.JdbcException;
import org.pbccrc.zsls.store.jdbc.SQLFormatter;
import org.pbccrc.zsls.store.jdbc.SqlTemplate;
import org.pbccrc.zsls.store.jdbc.Transaction;

/**
 */
public class DropTableSql {

    private static final Logger LOGGER = Logger.getLogger(DropTableSql.class);

    private SqlTemplate sqlTemplate;
    private Transaction trans;
    private StringBuilder sql = new StringBuilder();

    public DropTableSql(SqlTemplate sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
    }

    public DropTableSql inTransaction(Transaction trans) {
    	this.trans = trans;
    	return this;
    }
    
    public DropTableSql drop(String table) {
        sql.append("DROP TABLE ").append(table);
        return this;
    }
    
    public DropTableSql dropIfExist(String table) {
    	sql.append("DROP TABLE IF EXISTS ").append(table);
    	return this;
    }
    
    public boolean doDrop() {

        String finalSQL = sql.toString();

        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(SQLFormatter.format(finalSQL));
            }
            sqlTemplate.update(sql.toString(), trans);
        } catch (Exception e) {
            throw new JdbcException("Drop Table Error:" + SQLFormatter.format(finalSQL), e);
        }
        return true;
    }

}
