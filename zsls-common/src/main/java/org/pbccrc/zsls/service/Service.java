package org.pbccrc.zsls.service;

import org.pbccrc.zsls.config.Configuration;

public interface Service {
	
  public enum STATE {
    /** Constructed but not initialized */
    NOTINITED(0, "NOTINITED"),

    /** Initialized but not started or stopped */
    INITED(1, "INITED"),

    /** started and not stopped */
    STARTED(2, "STARTED"),

    /** stopped. No further state transitions are permitted */
    STOPPED(3, "STOPPED");

    private final int value;

    private final String statename;

    private STATE(int value, String name) {
      this.value = value;
      this.statename = name;
    }

    public int getValue() {
      return value;
    }

    @Override
    public String toString() {
      return statename;
    }
  }
  
  	void init(Configuration config);
  	
  	void start();
  	
  	void stop();
  	
  	void registerServiceListener(ServiceStateListener listener);
  	
  	void unregisterServiceListener(ServiceStateListener listener);

  	String getName();

  	Configuration getConfig();

  	STATE getServiceState();

  	long getStartTime();

  	boolean isInState(STATE state);

}
