package org.pbccrc.zsls.jobstore.zookeeper;

import java.util.HashMap;

public class ShardNode implements Comparable<ShardNode> {

	private boolean isCompleted = false;

	private String prefix = "shard";
	private transient long maxUnitID;
	private transient long minUnitID = -1;
	//k/v - unitid/node
	private transient HashMap<String, ScheduleUnitNode> shardChildren = new HashMap<String, ScheduleUnitNode>();
	
	private transient long id;
	
	private volatile int unitSize = 0;
	private boolean isFull = false;
	public ShardNode() {}
	public  ShardNode(long id) {
		this.id = id;
	}
	public boolean isCompleted() {
		return isCompleted;
	}

	public void setCompleted(boolean isCompleted) {
		this.isCompleted = isCompleted;
	}

	public int getUnitSize() {
		return unitSize;
	}

	public void setUnitSize(int unitSize) {
		this.unitSize = unitSize;
	}
	public void incUnitSize() {
		unitSize++;
	}
	public boolean isFull() {
		return isFull;
	}

	public void setFull(boolean isFull) {
		this.isFull = isFull;
	}

	@Override
	public String toString() {
		return prefix + id;
	}
	public HashMap<String, ScheduleUnitNode> getShardChildren() {
		return shardChildren;
	}
	public long getMaxUnitID() {
		return maxUnitID;
	}
	public void setMaxUnitID(long maxUnitID) {
//		this.maxUnitID = maxUnitID;
		if (this.maxUnitID < maxUnitID) {
			this.maxUnitID = maxUnitID;
		}
	}
	public long getMinUnitID() {
		return minUnitID;
	}
	public void setMinUnitID(long minUnitID) {
		if (this.minUnitID == -1) {
			this.minUnitID = minUnitID;
			return;
		}
		if (this.minUnitID > minUnitID) {
			this.minUnitID = minUnitID;
		}
	}
	@Override
	public int compareTo(ShardNode o) {
		// TODO Auto-generated method stub
		if (o == this) {
			return 0;
		}
		if (o.id == this.id) {
			return 0;
		} else {
			return id < o.id ? -1 : 1;
		}
	}
}
