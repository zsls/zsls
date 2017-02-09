package org.pbccrc.zsls.dataload;

public interface Updatable {
	
	boolean readyForUpdate();
	
	void doUpdate();

}
