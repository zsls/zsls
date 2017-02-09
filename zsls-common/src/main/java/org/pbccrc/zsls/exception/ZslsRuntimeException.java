package org.pbccrc.zsls.exception;

public class ZslsRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 7556185914544550516L;
	
	public ZslsRuntimeException (Exception e) {
		super(e);
	}
	
	public ZslsRuntimeException (String msg) {
		super(msg);
	}
	
	public ZslsRuntimeException (String msg, Exception e) {
		super(msg, e);
	}

}
