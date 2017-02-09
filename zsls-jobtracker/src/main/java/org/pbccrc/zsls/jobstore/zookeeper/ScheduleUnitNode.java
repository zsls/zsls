package org.pbccrc.zsls.jobstore.zookeeper;

public class ScheduleUnitNode {

	private int unitState = 0;

	private String prefix = "unit";
	private long id;
	public ScheduleUnitNode(long id) {
		this.id = id;
	}
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}
//	private transient UnitID unitID;
	public int getUnitState() {
		return unitState;
	}

	public void setUnitState(int unitState) {
		this.unitState = unitState;
	}

//	public UnitID getUnitID() {
//		return unitID;
//	}
//
//	public void setUnitID(UnitID unitID) {
//		this.unitID = unitID;
//	}
	@Override
	public String toString() {
		return prefix + id;
	}

}
