package org.pbccrc.zsls.store.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public interface Transaction {
	
	Connection getConn();
	
	void setConn(Connection conn);
	
	void commit() throws SQLException;
	
	void rollback() throws SQLException;
	
	void close() throws SQLException;

}
