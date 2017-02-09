package org.pbccrc.zsls.exception.store;

public class EntryNotExistException extends JdbcException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2424690314169320329L;

	public EntryNotExistException() {
		super();
	}
	
	public EntryNotExistException(String message) {
		super(message);
	}
	
	public EntryNotExistException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public EntryNotExistException(Throwable cause) {
		super(cause);
	}
}
