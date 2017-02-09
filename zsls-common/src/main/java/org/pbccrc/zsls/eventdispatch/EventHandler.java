package org.pbccrc.zsls.eventdispatch;

/**
 * Interface for handling events of type T
 *
 * @param <T> parameterized event of type T
 */
@SuppressWarnings("rawtypes")
public interface EventHandler<T extends Event> {

  void handle(T event);

}
