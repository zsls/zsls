package org.pbccrc.zsls.tasks.dt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.pbccrc.zsls.api.client.ITask;
import org.pbccrc.zsls.context.AppContext;

public class QuartzTaskManager {
	
	private static QuartzTaskManager instance;
	private static volatile boolean inited = false;
	private QuartzTaskManager(AppContext context) {
		this.context = context;
	}
	
	public static QuartzTaskManager getInstance() {
		if (!inited || instance == null)
			throw new IllegalArgumentException("QuartzTaskManager should be inited explicitely");
		return instance;
	}
	public synchronized static void init(AppContext context) {
		if (!inited) {
			instance = new QuartzTaskManager(context);
			inited = true;
		}
	}
	
	private AppContext context;
	public AppContext getContext() {
		return context;
	}
	
	private Map<String, ServerQuartzJob> jobs = new HashMap<String, ServerQuartzJob>();
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	public void putJob(String id, ServerQuartzJob job) {
		lock.writeLock().lock();
		try {
			jobs.put(id, job);
		} finally {
			lock.writeLock().unlock();
		}
	}
	public void deleteJob(String id) {
		lock.writeLock().lock();
		try {
			jobs.remove(id);
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	public ServerQuartzJob getJob(String id) {
		lock.readLock().lock();
		try {
			return jobs.get(id);
		} finally {
			lock.readLock().unlock();
		}
	}
	
	public ArrayList<ServerQuartzJob> getJobs() {
		ArrayList<ServerQuartzJob> list = new ArrayList<ServerQuartzJob>();
		lock.readLock().lock();
		try {
			for (ServerQuartzJob job : jobs.values())
				list.add(job);
			return list;
		} finally {
			lock.readLock().unlock();
		}
	}
	
	public String getJobWithTaskInDomain(String domain) {
		lock.readLock().lock();
		try {
			for (ServerQuartzJob job : jobs.values()) {
				for (Object o : job.getOrigJob().flowObjs) {
					if (o instanceof ITask && 
							domain.equals(((ITask)o).domain))
						return job.getJobId();
				}
			}
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}
	
}
