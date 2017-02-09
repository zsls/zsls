package org.pbccrc.zsls.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ZslsStateMachineFactory<STATE extends Enum<STATE>> {
	
	private Map<STATE, Set<STATE>> stateTable = new HashMap<STATE, Set<STATE>>();
	
	private STATE initState;
	
	private boolean build;
	
	public ZslsStateMachineFactory(STATE initState) {
		this.initState = initState;
	}
	
	public ZslsStateMachineFactory<STATE> addTransition(STATE src, STATE target) {
		if (build)
			throw new ZslsStateException("state machine factory already builded");
		Set<STATE> sets = stateTable.get(src);
		if (sets == null) {
			sets = new HashSet<STATE>();
			stateTable.put(src, sets);
		}
		sets.add(target);
		return this;
	}
	
	public ZslsStateMachineFactory<STATE> build() {
		build = true;
		return this;
	}
	
	private boolean transitionValid(STATE src, STATE target) {
		Set<STATE> sets = stateTable.get(src);
		return sets != null && sets.contains(target); 
	}
	
	public ZslsStateMachine<STATE> makeStateMachine() {
		if (!build)
			throw new ZslsStateException("state machine factory not builded yet");
		return new InnerStateMachine(initState);
	}
	
	
	/* inner state machine implementation */
	private class InnerStateMachine implements ZslsStateMachine<STATE> {
		STATE curState;
		InnerStateMachine(STATE initState) {
			this.curState = initState;
		}
		@Override
		public STATE getCurrentState() {
			return curState;
		}
		@Override
		public synchronized STATE doTransition(STATE target) {
			STATE ret = curState;
			if(ZslsStateMachineFactory.this.transitionValid(curState, target))
				curState = target;
			else
				ret = null;
			return ret;
		}
	}

}
