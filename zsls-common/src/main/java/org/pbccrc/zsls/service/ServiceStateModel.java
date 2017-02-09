package org.pbccrc.zsls.service;

import org.pbccrc.zsls.exception.ServiceStateException;

public class ServiceStateModel {
	
  /**
   * Map of all valid state transitions
   * [current] [proposed1, proposed2, ...]
   */
  private static final boolean[][] statemap =
    {
      //                uninited inited started stopped
      /* uninited  */    {false, true,  false,  true},
      /* inited    */    {false, true,  true,   true},
      /* started   */    {false, false, true,   true},
      /* stopped   */    {false, false, false,  true},
    };
	
  private volatile Service.STATE state;

  /**
   * The name of the service: used in exceptions
   */
  private String name;

  /**
   * Create the service state model in the {@link Service.STATE#NOTINITED}
   * state.
   */
  public ServiceStateModel(String name) {
    this(name, Service.STATE.NOTINITED);
  }

  /**
   * Create a service state model instance in the chosen state
   * @param state the starting state
   */
  public ServiceStateModel(String name, Service.STATE state) {
    this.state = state;
    this.name = name;
  }

  /**
   * Query the service state. This is a non-blocking operation.
   * @return the state
   */
  public Service.STATE getState() {
    return state;
  }

  /**
   * Query that the state is in a specific state
   * @param proposed proposed new state
   * @return the state
   */
  public boolean isInState(Service.STATE proposed) {
    return state.equals(proposed);
  }

  /**
   * Verify that that a service is in a given state.
   * @param expectedState the desired state
   * @throws ServiceStateException if the service state is different from
   * the desired state
   */
  public void ensureCurrentState(Service.STATE expectedState) {
    if (state != expectedState) {
      throw new ServiceStateException(name+ ": for this operation, the " +
                                      "current service state must be "
                                      + expectedState
                                      + " instead of " + state);
    }
  }

  /**
   * Enter a state -thread safe.
   *
   * @param proposed proposed new state
   * @return the original state
   * @throws ServiceStateException if the transition is not permitted
   */
  public synchronized Service.STATE enterState(Service.STATE proposed) {
    checkStateTransition(name, state, proposed);
    Service.STATE oldState = state;
    //atomic write of the new state
    state = proposed;
    return oldState;
  }

  /**
   * Check that a state tansition is valid and
   * throw an exception if not
   * @param name name of the service (can be null)
   * @param state current state
   * @param proposed proposed new state
   */
  public static void checkStateTransition(String name,
                                          Service.STATE state,
                                          Service.STATE proposed) {
    if (!isValidStateTransition(state, proposed)) {
      throw new ServiceStateException(name + " cannot enter state "
                                      + proposed + " from state " + state);
    }
  }

  /**
   * Is a state transition valid?
   * There are no checks for current==proposed
   * as that is considered a non-transition.
   *
   * using an array kills off all branch misprediction costs, at the expense
   * of cache line misses.
   *
   * @param current current state
   * @param proposed proposed new state
   * @return true if the transition to a new state is valid
   */
  public static boolean isValidStateTransition(Service.STATE current,
                                               Service.STATE proposed) {
    boolean[] row = statemap[current.getValue()];
    return row[proposed.getValue()];
  }

  /**
   * return the state text as the toString() value
   * @return the current state's description
   */
  @Override
  public String toString() {
    return (name.isEmpty() ? "" : ((name) + ": "))
            + state.toString();
  }

}
