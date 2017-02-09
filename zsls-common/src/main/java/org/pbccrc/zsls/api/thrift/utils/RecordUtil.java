package org.pbccrc.zsls.api.thrift.utils;

import java.util.Date;

import org.pbccrc.zsls.api.thrift.records.TCluster;
import org.pbccrc.zsls.api.thrift.records.TNodeId;
import org.pbccrc.zsls.api.thrift.records.TTaskId;
import org.pbccrc.zsls.api.thrift.records.TTaskInfo;
import org.pbccrc.zsls.api.thrift.records.TTaskResult;
import org.pbccrc.zsls.entry.ClusterInfo;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskInfo;
import org.pbccrc.zsls.entry.TaskResult;

public class RecordUtil {
	
	public static NodeId trans(TNodeId id) {
		return new NodeId(id.domain, id.name, id.ip, id.port);
	}
	
	public static TCluster trans(ClusterInfo info) {
		TCluster ret = new TCluster();
		ret.setMaster(info.getMasterAddr());
		return ret;
	}
	
	public static TaskResult trans(TTaskResult result, NodeId id) {
		TaskResult ret = new TaskResult();
		ret.setAction(result.getAction());
		ret.setAppendInfo(result.getInfo());
		ret.setNodeId(id);
		ret.setTaskId(trans(result.taskid));
		if (result.getGenerateTime() > 0)
			ret.setDate(new Date(result.getGenerateTime()));
		ret.setKeyMessage(result.keyMsg);
		ret.setRuntimeMeta(result.runtimeParams);
		return ret;
	}
	
	public static TaskId trans(TTaskId id) {
		return new TaskId(id.taskid);
	}
	
	public static TTaskInfo trans(TaskInfo info) {
		TTaskInfo tinfo = new TTaskInfo();
		tinfo.setData(info.getData());
		TTaskId id = new TTaskId();
		id.setTaskid(info.getTaskId().id);
		tinfo.setTaskid(id);
		tinfo.setGenerateTime(info.getGenerateTime());
		return tinfo;
	}

}
