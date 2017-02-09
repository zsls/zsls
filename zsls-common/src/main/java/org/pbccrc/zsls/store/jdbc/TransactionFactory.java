package org.pbccrc.zsls.store.jdbc;

public class TransactionFactory {
	
	public static Transaction getTransaction() {	
		Transaction tx = new TransactionImpl();
		return tx;
	}
	
}
