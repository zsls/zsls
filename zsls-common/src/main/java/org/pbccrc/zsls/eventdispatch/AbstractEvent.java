package org.pbccrc.zsls.eventdispatch;

/**
 * Parent class of all the events. All events extend this class.
 */
public abstract class AbstractEvent<TYPE extends Enum<TYPE>> 
    implements Event<TYPE> {

  private final TYPE type;
  private final long timestamp;

  // use this if you DON'T care about the timestamp
  public AbstractEvent(TYPE type) {
    this.type = type;
    // We're not generating a real timestamp here.  It's too expensive.
    timestamp = -1L;
  }

  // use this if you care about the timestamp
  public AbstractEvent(TYPE type, long timestamp) {
    this.type = type;
    this.timestamp = timestamp;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public TYPE getType() {
    return type;
  }

  @Override
  public String toString() {
    return "EventType: " + getType();
  }
}
