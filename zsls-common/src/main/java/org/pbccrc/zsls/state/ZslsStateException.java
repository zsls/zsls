package org.pbccrc.zsls.state;

import org.pbccrc.zsls.exception.ZslsRuntimeException;

public class ZslsStateException extends ZslsRuntimeException {

	public ZslsStateException(String msg, Exception e) {
		super(msg, e);
	}
	public ZslsStateException(String msg) {
		super(msg);
	}
	public ZslsStateException(Exception e) {
		super(e);
	}

	private static final long serialVersionUID = 2535092402270857812L;

}
