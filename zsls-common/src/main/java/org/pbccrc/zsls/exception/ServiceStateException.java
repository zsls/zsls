package org.pbccrc.zsls.exception;

public class ServiceStateException extends RuntimeException {
	
	private static final long serialVersionUID = 4461876687423036507L;

	public ServiceStateException(String message) {
		super(message);
	}

	public ServiceStateException(String message, Throwable cause) {
		super(message, cause);
	}

	public ServiceStateException(Throwable cause) {
		super(cause);
	}

}
