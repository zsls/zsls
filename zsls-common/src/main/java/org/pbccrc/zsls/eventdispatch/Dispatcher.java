package org.pbccrc.zsls.eventdispatch;

/**
 * Event Dispatcher interface. It dispatches events to registered 
 * event handlers based on event types.
 * 
 */
@SuppressWarnings("rawtypes")
public interface Dispatcher {

  // Configuration to make sure dispatcher crashes but doesn't do system-exit in
  // case of errors. By default, it should be false, so that tests are not
  // affected. For all daemons it should be explicitly set to true so that
  // daemons can crash instead of hanging around.
  public static final String DISPATCHER_EXIT_ON_ERROR_KEY =
      "yarn.dispatcher.exit-on-error";

  public static final boolean DEFAULT_DISPATCHER_EXIT_ON_ERROR = false;

  EventHandler getEventHandler();

  void register(Class<? extends Enum> eventType, EventHandler handler);

}
