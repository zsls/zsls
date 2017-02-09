package org.pbccrc.zsls.tasks;

import java.util.Map;

import org.pbccrc.zsls.collection.CopyOnWriteHashMap;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.jobengine.JobManager;

public class LocalJobManager {
	
	private static Map<String, JobManager> rtJobManagers = new CopyOnWriteHashMap<String, JobManager>();
	private static JobManager dtJobManager = new JobManager();
	
	public static JobManager addRTJobManager(String domain) {
		JobManager manager = new JobManager();
		rtJobManagers.put(domain, manager);
		return manager;
	}
	
	public static JobManager delRTJobManager(String domain) {
		return rtJobManagers.remove(domain);
	}
	
	public static JobManager getRTJobManager(String domain) {
		return rtJobManagers.get(domain);
	}
	
	public static JobManager getDTJobManager() {
		return dtJobManager;
	}
	
	public static JobManager getJobManager(String domain, DomainType dtype) {
		if (dtype == DomainType.RT)
			return rtJobManagers.get(domain);
		else
			return dtJobManager;
	}
}
