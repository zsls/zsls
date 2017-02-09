package org.pbccrc.zsls.api.client.old;

public class UnitValidator {
	
	public static boolean checkUnitValid(IScheduleUnit unit) {
		if (unit == null) return false;
		if (unit.independentTasks.size() == 0 && unit.relations.size() == 0)
			return false;
		for (IUserTask task : unit.independentTasks) {
			if (task.id == null)
				return false;
		}
		for (IRelation relation : unit.relations) {
			if (relation.preTasks.id == null || relation.postTasks.id == null)
				return false;
			if (relation.preTasks.tasks.size() == 0 || relation.postTasks.tasks.size() == 0)
				return false;
			for (IUserTask task : relation.preTasks.tasks) {
				if (task.id == null) return false;
			}
			for (IUserTask task : relation.postTasks.tasks) {
				if (task.id == null) return false;
			}
		}
		return true;
	}

}
