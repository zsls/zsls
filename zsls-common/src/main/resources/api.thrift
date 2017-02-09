include "records.thrift"

namespace java org.pbccrc.zsls.api.thrift

service InnerTrackerProtocol {
  records.RegisterResponse regiserNode(1:records.RegisterRequest request);
  records.HeartBeatResponse heartBeat(1:records.HeartBeatRequest request);
  records.ReportTaskResponse taskComplete(1:records.ReportTaskRequest request);
}

service TaskHandleProtocol {
  oneway void assignTask(1:records.TaskHandleRequest request);
  oneway void killTask(1:records.TaskHandleRequest request);
}