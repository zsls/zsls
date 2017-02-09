package org.pbccrc.zsls.store.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public class TransactionImpl implements Transaction {
	
	private Connection conn;
	
	public Connection getConn() {
		return conn;
	}
	
	public void setConn(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void commit() throws SQLException {
		if (conn != null)
			conn.commit();
	}

	@Override
	public void rollback() throws SQLException {
		if (conn != null)
			conn.rollback();
	}

	@Override
	public void close() throws SQLException {
		if (conn != null)
			conn.close();
	}

}
