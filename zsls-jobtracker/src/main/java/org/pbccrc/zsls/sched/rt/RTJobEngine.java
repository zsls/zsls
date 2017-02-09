package org.pbccrc.zsls.sched.rt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	public RTJobEngine(String domain, JobManager manager, int cacheLimit, double factor) {
		super(manager);
		this.domain = domain;
		this.cacheLimit = Math.max(cacheLimit, ZslsConstants.MIN_TASK_CACHE);
		this.threshold = (int) (cacheLimit * factor);
		this.relatedUnits = new HashMap<RTJobId, List<JobFlow>>(128);
	}

	public int feed(JobFlow unit) {
		RTJobFlow u = (RTJobFlow) unit;
		// whether depend on previous units or not.
		if (u.getPreUnit() != null && manager.getUnit(u.getPreUnit().toString()) != null) {
			List<JobFlow> list = relatedUnits.get(u.getPreUnit());
			if (list == null) {
				list = new ArrayList<JobFlow>(2);
				relatedUnits.put(u.getPreUnit(), list);
			}
			list.add(u);
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
			if (units != null) {
				for (JobFlow u : units) {
					this.feed(u);
				}
			}
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
