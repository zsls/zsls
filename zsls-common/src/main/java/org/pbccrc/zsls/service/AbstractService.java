package org.pbccrc.zsls.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.exception.ServiceStateException;

public abstract class AbstractService implements Service {
	private static Logger LOG = Logger.getLogger(AbstractService.class);
	
	private final String name;
	
	private final ServiceStateModel stateModel;
	
	private volatile Configuration config;
	
	private long startTime;
	
	private List<ServiceStateListener> listeners;
	
	private final Object stateChangeLock = new Object();
	
	public AbstractService(String name) {
		this.name = name;
		stateModel = new ServiceStateModel(name);
		listeners = new ArrayList<ServiceStateListener>();
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public synchronized Configuration getConfig() {
		return config;
	}

	@Override
	public long getStartTime() {
		return startTime;
	}
	
	@Override
	public void registerServiceListener(ServiceStateListener listener) {
		if (!listeners.contains(listener))
			listeners.add(listener);
	}
	
	@Override
	public void unregisterServiceListener(ServiceStateListener listener) {
		listeners.remove(listener);
	}
	
	@Override
	public void init(Configuration conf) {
		if (conf == null) {
			throw new ServiceStateException("Cannot initialize service " + getName() + ": null configuration");
		}
		if (isInState(STATE.INITED)) {
			return;
		}
		synchronized (stateChangeLock) {
			if (enterState(STATE.INITED) != STATE.INITED) {
				setConfig(conf);
				try {
					serviceInit(config);
					if (isInState(STATE.INITED)) {
						// if the service ended up here during init,
						// notify the listeners
						notifyListeners();
					}
				} catch (Exception e) {
					throw new ServiceStateException(e.getMessage(), e);
				}
			}
		}
	}
	
	@Override
	public void start() {
		if (isInState(STATE.STARTED)) {
			return;
		}
		// enter the started state
		synchronized (stateChangeLock) {
			if (stateModel.enterState(STATE.STARTED) != STATE.STARTED) {
				try {
					startTime = System.currentTimeMillis();
					serviceStart();
					if (isInState(STATE.STARTED)) {
						// if the service started (and isn't now in a later
						// state), notify
						if (LOG.isDebugEnabled()) {
							LOG.debug("Service " + getName() + " is started");
						}
						notifyListeners();
					}
				} catch (Exception e) {
					throw new ServiceStateException(e.getMessage(), e);
				}
			}
		}
	}

	@Override
	public void stop() {
		if (isInState(STATE.STOPPED)) {
			return;
		}
		synchronized (stateChangeLock) {
			if (enterState(STATE.STOPPED) != STATE.STOPPED) {
				try {
					serviceStop();
				} catch (Exception e) {
					throw new ServiceStateException(e.getMessage(), e);
				} finally {
					// notify anything listening for events
					notifyListeners();
				}
			} else {
				// already stopped: note it
				if (LOG.isDebugEnabled()) {
					LOG.debug("Ignoring re-entrant call to stop()");
				}
			}
		}
	}
	
	private void notifyListeners() {
		for (ServiceStateListener listener : listeners) {
			listener.serviceStateChanged(this);
		}
	}
	
	protected void setConfig(Configuration conf) {
	    this.config = conf;
	}

	private STATE enterState(STATE newState) {
		assert stateModel != null : "null state in " + name + " " + this.getClass();
		STATE oldState = stateModel.enterState(newState);
		if (oldState != newState) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Service: " + getName() + " entered state " + getServiceState());
			}
		}
		return oldState;
	}
	
	public STATE getServiceState() {
		return stateModel.getState();
	}
	
	@Override
	public final boolean isInState(Service.STATE expected) {
		return stateModel.isInState(expected);
	}

	@Override
	public String toString() {
		return "Service " + name + " in state " + stateModel;
	}
	
	/*-----------------------------------*/
	
	protected void serviceInit(Configuration conf) throws Exception {
		
	}
	
	protected void serviceStart() throws Exception {
		
	}
	
	protected void serviceStop() throws Exception {
		
	}

}
