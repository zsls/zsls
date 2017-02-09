package org.pbccrc.zsls.exception;

public class SchedException extends Exception{

	private static final long serialVersionUID = 7444088517792995228L;

	public SchedException(Exception e) {
		e.printStackTrace();
	}
}
