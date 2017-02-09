package org.pbccrc.zsls.dataload;

import java.util.ArrayList;
import java.util.List;

import org.pbccrc.zsls.utils.DomainLogger;

public class DataLoader extends Thread {
	public static final long DEFAULT_LOAD_INTERVAL = 10 * 60 * 1000;
	
	private static DomainLogger L = DomainLogger.getLogger(DataLoader.class.getSimpleName());
	
	private long interval = DEFAULT_LOAD_INTERVAL;
	
	private volatile boolean stop;
	
	private List<Updatable> sources = new ArrayList<Updatable>();
	
	
	public void setInterval(long interval) {
		this.interval = interval;
	}
	
	public void addSource(Updatable source) {
		sources.add(source);
		L.info(null, "add source " + source);
	}
	
	public void loadOnceAndStart() {
		for (Updatable source : sources) {
			if (source.readyForUpdate()) {
				source.doUpdate();
			}
		}
		start();
	}
	
	public void run() {
		while (!stop) {
			for (Updatable source : sources) {
				if (source.readyForUpdate()) {
					source.doUpdate();
				}
			}
			try {
				Thread.sleep(interval);
			} catch (InterruptedException ignore) {
			}
		}
	}
	
	public void stopLoad() {
		stop = true;
	}

}
