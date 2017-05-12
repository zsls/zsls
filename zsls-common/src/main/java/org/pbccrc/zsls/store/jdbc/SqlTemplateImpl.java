package org.pbccrc.zsls.store.jdbc;

import javax.sql.DataSource;

import org.pbccrc.zsls.store.jdbc.utils.DbRunner;
import org.pbccrc.zsls.store.jdbc.utils.ResultSetHandler;
import org.pbccrc.zsls.store.jdbc.utils.ScalarHandler;

import java.sql.Connection;
import java.sql.SQLException;

/**
 */
class SqlTemplateImpl implements SqlTemplate {

    private final DataSource dataSource;
    private final static DbRunner dbRunner = new DbRunner();

    public SqlTemplateImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Connection getConn(boolean autoCommit) throws SQLException {
    	Connection conn = dataSource.getConnection();
    	conn.setAutoCommit(autoCommit);
    	return conn;
    }

    private void close(Connection conn) throws SQLException {
        if (conn != null) {
            if (conn.isReadOnly()) {
                conn.setReadOnly(false);  // restore NOT readOnly before return to pool
            }
            if (!conn.getAutoCommit())
            	conn.setAutoCommit(true);
            conn.close();
        }
    }

    public void createTable(final String sql) throws SQLException {
        update(sql);
    }
    
    public void createSequence(final String sql) throws SQLException {
    	update(sql);
    }
    
    @Override
    public int[] batchInsert(String sql, Object[][] params) throws SQLException {
        return batchUpdate(sql, params);
    }

    public int[] batchUpdate(final Connection conn, final String sql, final Object[][] params) throws SQLException {
        return dbRunner.batch(conn, sql, params);
    }

    public int[] batchUpdate(final String sql, final Object[][] params) throws SQLException {
    	Connection conn = getConn(true);
    	try {
    		return batchUpdate(conn, sql, params);
    	} finally {
    		close(conn);
    	}
    }

    @Override
    public int insert(String sql, Object... params) throws SQLException {
        return update(sql, null, params);
    }
    
    @Override
    public int insert(String sql, Transaction trans, Object... params) throws SQLException {
        return update(sql, trans, params);
    }
    
    @Override
    public int update(final String sql, final Object... params) throws SQLException {
    	return update(sql, null, params);
    }

    @Override
    public int update(final String sql, Transaction trans, final Object... params) throws SQLException {
    	if (trans != null) {
    		Connection conn = trans.getConn();
    		if (conn == null) {
    			conn = getConn(false);
    			trans.setConn(conn);
    		}
    		return update(conn, sql, params);
    	} else {
    		Connection conn = getConn(true);
    		try {
    			return update(conn, sql, params);
    		} finally {
    			close(conn);
    		}
    	}
    }

    @Override
    public int delete(String sql, Object... params) throws SQLException {
        return update(sql, null, params);
    }
    
    @Override
    public int delete(String sql, Transaction trans, Object... params) throws SQLException {
        return update(sql, trans, params);
    }

    public int update(final Connection conn, final String sql, final Object... params) throws SQLException {
        return dbRunner.update(conn, sql, params);
    }

    public <T> T query(final String sql, Transaction trans, final ResultSetHandler<T> rsh, final Object... params) throws SQLException {
    	Connection conn = null;
    	if (trans == null) {
    		conn = getConn(true);
	    	try {
	    		return query(conn, sql, rsh, params);
	    	} finally {
	    		close(conn);
	    	}	
    	}
    	else {
    		conn = trans.getConn();
    		if (conn == null) {
    			conn = getConn(false);
    			trans.setConn(conn);
    		}
	    	return query(conn, sql, rsh, params);
    	}
    }
    
    public <T> T query(final String sql, final ResultSetHandler<T> rsh, final Object... params) throws SQLException {
        return query(sql, null, rsh, params);
    }

    public <T> T query(final Connection conn, final String sql, final ResultSetHandler<T> rsh, final Object... params) throws SQLException {
        return dbRunner.query(conn, sql, rsh, params);
    }

    public <T> T queryForValue(final String sql, final Object... params) throws SQLException {
        return query(sql, new ScalarHandler<T>(), params);
    }

    public <T> T queryForValue(final Connection conn, final String sql, final Object... params) throws SQLException {
        return query(conn, sql, new ScalarHandler<T>(), params);
    }

    private SqlExecutor<Void> getWrapperExecutor(final SqlExecutorVoid voidExecutor) {
        return new SqlExecutor<Void>() {
            @Override
            public Void run(Connection conn) throws SQLException {
                voidExecutor.run(conn);
                return null;
            }
        };
    }

    public void executeInTransaction(SqlExecutorVoid executor) {
        executeInTransaction(getWrapperExecutor(executor));
    }

    public <T> T executeInTransaction(SqlExecutor<T> executor) {
        Connection conn = null;
        try {
            conn = TxConnectionFactory.getTxConnection(dataSource);
            T res = executor.run(conn);
            conn.commit();
            return res;
        } catch (Exception e) {
            throw rollback(conn, e);
        } finally {
            TxConnectionFactory.closeTx(conn);
        }
    }

    private StateException rollback(Connection conn, Throwable e) {
        try {
            if (conn != null) {
                conn.rollback();
            }
            return new StateException(e);
        } catch (SQLException se) {
            return new StateException("Unable to rollback transaction", e);
        }
    }

}
