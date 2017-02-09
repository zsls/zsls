package org.pbccrc.zsls.tasks.rt;

import org.pbccrc.zsls.jobengine.JobId;

public class RTJobId extends JobId implements Comparable<RTJobId> {
	public static String prefix = "unit";
	
	long id;
	public RTJobId(long id) {
		this.id = id;
	}
	public long getId() {
		return id;
	}
	
	public RTJobId nextID() {
		return new RTJobId(id + 1);
	}
	
	public boolean laterThan(RTJobId u) {
		return this.id > u.id;
	}
	
	public boolean noEerlierThan(RTJobId u) {
		return this.id >= u.id;
	}
	
	public static RTJobId fromString(String in) {
		if (in != null && in.startsWith(prefix)) {
			try {
				int end = in.indexOf("-");
				end = end > 0 ? end : in.length();
				long id = Long.parseLong(in.substring(prefix.length(), end));
				return new RTJobId(id);
			} catch (Exception ignore) {
			}
		}
		return null;
	}
	
	public static String toString(long id) {
		return prefix + id;
	}
	
	public String toString() {
		return prefix + id;
	}
	
	public boolean equals(Object o) {
		if (o == this) 
			return true;
		if (!(o instanceof RTJobId))
			return false;
		RTJobId id = (RTJobId)o;
		return id.id == this.id;
	}
	
	@Override
	public int compareTo(RTJobId o) {
		if (o == this)
			return 0;
		if (id == o.id)
			return 0;
		else
			return id < o.id ? -1 : 1;
	}
	
	@Override
	public int hashCode() {
		int ret = new Long(id).hashCode();
		return ret;
	}
}
