package org.pbccrc.zsls.state;

public interface ZslsStateMachine<STATE extends Enum<STATE>> {
	
	STATE getCurrentState();
	
	/*
	 * return former state if successful; null if fails
	 */
	STATE doTransition(STATE target);

}
