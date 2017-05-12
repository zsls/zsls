package org.pbccrc.zsls.sched.rt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.jobengine.JobEngine;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.JobManager;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.utils.DomainLogger;

public class RTJobEngine extends JobEngine {
	private static final DomainLogger L = DomainLogger.getLogger(RTJobEngine.class.getSimpleName());

	private String domain;
	private int cacheLimit;
	private int threshold;
	private boolean sync;
	private Map<RTJobId, List<JobFlow>> relatedUnits;
	private Set<RTJobId> forbidSet;
	
	private boolean forbid(RTJobId jobId) {
		return forbidSet.add(jobId);
	}
	private boolean cancelForbid(RTJobId jobId) {
		return forbidSet.remove(jobId);
	}
	private boolean forbidden(RTJobId jobId) {
		return forbidSet.contains(jobId);
	}
	
	public RTJobEngine(String domain, JobManager manager, int cacheLimit, double factor) {
		super(manager);
		this.domain = domain;
		this.cacheLimit = Math.max(cacheLimit, ZslsConstants.MIN_TASK_CACHE);
		this.threshold = (int) (cacheLimit * factor);
		this.relatedUnits = new HashMap<RTJobId, List<JobFlow>>(128);
		this.forbidSet = new HashSet<RTJobId>();
	}

	public int feed(JobFlow unit) {
		if (unit.isFinished())
			return 0;
		RTJobFlow u = (RTJobFlow) unit;
		// whether depend on previous units or not.
		RTJobId preUnit = u.getPreUnit();
		if (preUnit != null && (manager.getUnit(preUnit.toString()) != null || forbidden(preUnit))) {
			List<JobFlow> list = relatedUnits.get(u.getPreUnit());
			if (list == null) {
				list = new ArrayList<JobFlow>(2);
				relatedUnits.put(u.getPreUnit(), list);
			}
			list.add(u);
			forbid(u.getJobId());
			return 1;
		}
		return super.feed(unit);
	}

	public JobFlow complete(TaskId id, TaskResult ret) {
		JobFlow unit = super.complete(id, ret);
		if (unit != null) {
			L.info(domain, "unit " + unit.getJobId() + " finished");
			manager.unregister(unit);
			List<JobFlow> units = relatedUnits.get(unit.getJobId());
			cancelForbid((RTJobId)unit.getJobId());
			if (units != null) {
				for (JobFlow u : units) {
					this.feed(u);
				}
			}
			relatedUnits.remove(unit.getJobId());
		}
		return unit;
	}

	public boolean canReload() {
		return manager.getTaskCacheSize() < threshold;
	}

	public int getLoadCapacity() {
		return cacheLimit - manager.getTaskCacheSize();
	}

	public String getDomain() {
		return domain;
	}

	public boolean isSync() {
		return sync;
	}

	public void setSync(boolean sync) {
		this.sync = sync;
	}

}
