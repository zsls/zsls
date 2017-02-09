package org.pbccrc.zsls.utils;

import org.apache.log4j.Logger;

public class DomainLogger {
	public static final String SYS = "#######";
	
	public static DomainLogger getLogger(String clazzname) {
		org.apache.log4j.Logger l = org.apache.log4j.Logger.getLogger(clazzname);
		return new DomainLogger(l);
	}
	
	private org.apache.log4j.Logger log;
	
	public DomainLogger(org.apache.log4j.Logger log) {
		this.log = log;
	}
	
	public Logger logger() {
		return log;
	}
	
	public void trace(String domain, Object msg) {
		log.trace("[" + domain + "] " + msg);
	}
	
	public void debug(String domain, Object msg) {
		log.debug("[" + domain + "] " + msg);
	}
	
	public void warn(String domain, Object msg) {
		log.warn("[" + domain + "] " + msg);
	}
	
	public void error(String domain, Object msg) {
		log.error("[" + domain + "] " + msg);
	}
	
	public void fatal(String domain, Object msg) {
		log.fatal("[" + domain + "] " + msg);
	}
	
	public void info(String domain, Object msg) {
		log.info("[" + domain + "] " + msg);
	}

}
