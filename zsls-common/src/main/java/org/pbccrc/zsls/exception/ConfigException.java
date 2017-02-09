package org.pbccrc.zsls.exception;

public class ConfigException extends RuntimeException {

	private static final long serialVersionUID = 494414200651379567L;
	
	public ConfigException(String message) {
		super(message);
	}

	public ConfigException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConfigException(Throwable cause) {
		super(cause);
	}
	
}
