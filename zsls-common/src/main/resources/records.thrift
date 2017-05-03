namespace java org.pbccrc.zsls.api.thrift.records

struct TNodeId {
  1: required string ip;
  2: required i32 port;
  3: required string name;
  4: required string domain;
}

struct TTaskId {
  1: required string taskid;
}

struct TCluster {
  1: required string master;
}

struct TTaskInfo {
  1: required TTaskId taskid;
  2: required map<string, string> data;
  3: optional i64 generateTime;
}

struct TTaskResult {
  1: required TaskAction action;
  2: required TTaskId taskid;
  3: optional i64 executeTime;
  4: optional string info;
  5: optional string keyMsg;
  6: optional map<string, string> runtimeParams;
  7: optional i64 generateTime;
}

enum NodeAction {
  NORMAL,
  NOT_MASTER,
  INVALID,
  RE_REGISTER
}

enum TaskAction {
  COMPLETE,
  FAILED,
  QUARTZ_COMPLETE,
  QuARTZ_FAILED,
  ACCEPT,
  DENIED
}

enum TaskType {
  NORMAL,
  TRIGGER
}

struct RegisterRequest {
  1: required string domain;
  2: required TNodeId nodeid;
  3: required i32 maxnum;
  4: optional bool isDt;
  5: optional list<TTaskId> runningTasks;
  6: optional list<TTaskResult> taskResults;
}

struct RegisterResponse {
  1: required NodeAction nodeAction;
  2: required i32 heartBeatInterval;
  3: required i32 registrySessTimeout;
  4: optional string message;
  5: optional TCluster cluster;
}

struct HeartBeatRequest {
  1: required string domain;
  2: required TNodeId nodeid;
  3: required list<TTaskId> runningTasks;
}

struct HeartBeatResponse {
  1: required NodeAction nodeAction;
  2: optional TCluster cluster;
}

struct ReportTaskRequest {
  1: required string domain;
  2: required TNodeId nodeId;
  3: required list<TTaskResult> taskResults;
  4: required list<TTaskId> runningTasks;
}

struct ReportTaskResponse {
  1: required NodeAction nodeAction;
  2: optional list<TTaskInfo> newTasks;
}

struct WNQueryRequest {
  1: optional TCluster cluster;
}

struct TaskHandleRequest {
  1: required TTaskInfo taskInfo;
  2: required TaskType taskType;
  3: required bool retryTask;
}

struct TaskHandleResponse {
  1: required TaskAction action;
}
