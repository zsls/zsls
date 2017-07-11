package org.pbccrc.zsls.jobstore.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.entry.TaskId;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.jobengine.JobFlow;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.jobengine.Task.ExecuteResult;
import org.pbccrc.zsls.jobengine.Task.TaskStat;
import org.pbccrc.zsls.jobstore.JobStore;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobFlow.RJobStat;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.tasks.rt.RTTask;
import org.pbccrc.zsls.utils.JsonSerilizer;
import org.pbccrc.zsls.utils.TaskUtil;
import org.pbccrc.zsls.utils.ZKOperateHelper;
import org.pbccrc.zsls.utils.core.AbstractIdGenerator;

public class ZookeeperJobStore implements JobStore {
	
	public static final String INCREMENT_ID_PATH	= "/generator";
	
	protected class IDGenerator extends AbstractIdGenerator {
		public IDGenerator() throws Exception {
			super();
		}
		@Override
		public void updateLimit(long limit) throws Exception {
			byte[] data = String.valueOf(limit).getBytes();
			zkclient.setData().forPath(INCREMENT_ID_PATH, data);
		}
		@Override
		public long fetchLimit() throws Exception {
			byte[] data = zkclient.getData().forPath(INCREMENT_ID_PATH);
			String dataString = new String(data);
			long id = Long.valueOf(dataString);
			return id;
		}
	}
	
	private CuratorFramework zkclient;
	
	private IDGenerator generator;
	private JobStoreZkUtils jobStoreZkUtils;
	
	public ZookeeperJobStore() {
		try {
			init();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
		jobStoreZkUtils = JobStoreZkUtils.getInstance(zkclient);
		try {
			generator = null;
			generator = new IDGenerator();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//over
	@Override
	public long instoreJob(RTJobFlow unit, IScheduleUnit origUnit) {
		
		long sequenceId = -1;
		String shard;
		String domain = unit.getDomain();
		try {
			sequenceId = generator.generateId();
			shard = jobStoreZkUtils.getDomainAvailableShard(domain);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
		
		int taskNum = unit.getTaskNum();
		String basePath = ZslsConstants.ZK_ROOT + "/" + domain +"/" + shard; 
		String scheduleUnitPath = basePath + "/"+ "unit" + sequenceId;
		ScheduleUnitNode scheduleUnitNode = new ScheduleUnitNode(sequenceId);
		//创建scheduleUnit节点设置数据
		
		String unitchild = JsonSerilizer.serilize(origUnit);
		/*创建scheduleUnit节点
		添加一个unit节点
		添加众多task节点
		修改unit的id，task的id
		*/
		try {
			zkclient.create()
					.creatingParentsIfNeeded()
					.forPath(scheduleUnitPath, JsonSerilizer.serilize(scheduleUnitNode).getBytes());
			
			String unitPath = scheduleUnitPath + "/" + "unit";
			String taskPath = scheduleUnitPath + "/" + "task";
			String unitChildPath = unitPath + "/" + unitchild;
			UnitNode unitNode = new UnitNode();
			unitNode.setTaskNum(taskNum);
			
			TaskNode taskNode = new TaskNode();
			
			/*创建unit 和 task 两个节点*/
			CuratorTransactionBridge transactionBridge = zkclient.inTransaction()
					.create().forPath(unitPath, JsonSerilizer.serilize(unitNode).getBytes())
					.and().create().forPath(taskPath, JsonSerilizer.serilize(taskNode).getBytes());
			/*创建unit 下的child内容，只有一个child*/
			transactionBridge = transactionBridge.and().create().forPath(unitChildPath);
			
			/*创建task节点下的children 可能会有多个child*/
			unit.updateUnitIdForAllTasks(sequenceId);
			Iterator<Task> taskIterator = unit.getTaskIterator();
			while(taskIterator.hasNext()) {
				RTTask task = (RTTask)taskIterator.next();
				String taskId = task.getTaskId();
				UserTaskChild child = new UserTaskChild(unit, task);
				String userTaskNodeData = JsonSerilizer.serilize(child);
				System.out.println(userTaskNodeData);
				String path = taskPath + "/" + taskId;
				
				//create task_id 节点和节点上的数据
				transactionBridge = transactionBridge.and().create().forPath(path, userTaskNodeData.getBytes());
			}
			transactionBridge.and().commit();
			
			jobStoreZkUtils.addUnit(domain, shard, new RTJobId(sequenceId) , scheduleUnitNode);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		return sequenceId;
	}
	//over
	@Override
	public boolean updateTask(String domain, TaskId task, TaskStat taskStat, ExecuteResult r) {
		if (domain == null || task == null || taskStat == null) {
			throw new IllegalArgumentException("argument is null");
		}
		String unitId = parserTaskIdAndGetUnitIndex(task);
		long idSequence = parserUnitIdAndGetIndex(unitId);
		String shard = jobStoreZkUtils.locateUnitShardPath(domain, new RTJobId(idSequence));
		String taskId = task.id;
		/*指向task 的child节点的路径*/
		String basePath = ZslsConstants.ZK_ROOT + "/" + domain + "/" + shard
				+ "/" + unitId + "/" + "task" + "/" + taskId;
		try {
			Stat nodeStat = zkclient.checkExists().forPath(basePath);
			if (nodeStat == null) {
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		try {
			byte[] bytes = zkclient.getData().forPath(basePath);
			if (bytes == null || bytes.length == 0) {
				return false;
			}
			//序列化得到的数据，修改状态，回写zk
			//1.得到数据
			UserTaskChild userTaskChild = JsonSerilizer.deserilize(new String(bytes), UserTaskChild.class);
			//2.修改状态
			userTaskChild.setTaskStat(taskStat);
			//3.回写
			zkclient.setData().forPath(basePath, JsonSerilizer.serilize(userTaskChild).getBytes());
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	//over
	@Override
	public RTJobFlow fetchJob(String domain, RTJobId id) {
		/*指向scheduleUnit 节点的路径*/
		
		String shard = jobStoreZkUtils.locateUnitShardPath(domain, id);
		String unitId = id.toString();
		String basePath = ZslsConstants.ZK_ROOT + "/" + domain + "/" + shard
				+ "/" + unitId + "/" + "unit";
		RTJobFlow scheduleUnit = null;
		try {
			List<String> children = zkclient.getChildren().forPath(basePath);
			if (children.size() == 1) {
				IScheduleUnit iScheduleUnit = JsonSerilizer.deserilize(children.get(0), IScheduleUnit.class);
				scheduleUnit = TaskUtil.parseJobUnit(iScheduleUnit);
				scheduleUnit.setUnitId(id.getId());
				scheduleUnit.updateUnitIdForAllTasks(id.getId());
				return scheduleUnit;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	//over
	@Override
	public boolean isJobFinish(String domain, RTJobId id) {
		String shard = jobStoreZkUtils.locateUnitShardPath(domain, id);
		String unitId = id.toString();
		String basePath = ZslsConstants.ZK_ROOT + "/" + domain + "/" + shard
				+ "/" + unitId;
		try {
			byte[] bytes = zkclient.getData().forPath(basePath);
			ScheduleUnitNode scheduleUnitNode = JsonSerilizer.deserilize(
					new String(bytes), ScheduleUnitNode.class);
			return scheduleUnitNode.getUnitState() == 1;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("zkClient getData error ...", e);
		}
	}
	/*
	 * 用到内存缓存
	 * over
	 */
	@Override
	public RTJobId getLastUnit(String domain) {

		long unitID = jobStoreZkUtils.getMaxIdUnit(domain);
		return new RTJobId(unitID);
	}

	//done
	@Override
	public RTJobId getFirstUnfinishedUnit(String domain) {
		String domainBase = ZslsConstants.ZK_ROOT + "/" + domain ;
		ScheduleUnitNode firstUnitNode = null;
		//得到domain下的shard children
//		HashMap<long, String> 
		List<ShardNode> shardNodes = new LinkedList<ShardNode>();
		try {
			List<String> shardsList = zkclient.getChildren().forPath(domainBase);
			for (int i = 0; i < shardsList.size(); i++) {
				long index = parserShardAndGetIndex(shardsList.get(i));
				ShardNode item = new ShardNode((int)index);
				shardNodes.add(item);
			}
			//将shard按大小顺序排序
			if (shardNodes.size() > 0) {
				Collections.sort(shardNodes);
			}
			for (int i = 0; i < shardNodes.size(); i++) {
				String shardStr = shardNodes.get(i).toString();
				String shardBase = domainBase + "/" + shardStr;
				List<String> units = zkclient.getChildren().forPath(shardBase);
				for (int j = 0; j < units.size(); j++) {
					String unitBase = shardBase + "/" + units.get(j);
					byte[] bytes = zkclient.getData().forPath(unitBase);
					ScheduleUnitNode unitNode = JsonSerilizer.deserilize(new String(bytes), ScheduleUnitNode.class);
					if(unitNode.getUnitState() == 0 ) {
						long unitId = parserUnitIdAndGetIndex(units.get(j));
						unitNode.setId(unitId);
						if (firstUnitNode == null) {
							firstUnitNode = unitNode;
						} else {
							if (unitNode.getId() < firstUnitNode.getId()) {
								firstUnitNode = unitNode;
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (firstUnitNode == null) {
			return null;
		}
		return new RTJobId(firstUnitNode.getId());
	}
	
	public List<RTJobFlow> fetchJobs(String domain, RTJobId unitid, int taskLimit) {
		long startIndex = unitid.getId();
		List<RTJobFlow> list = new ArrayList<RTJobFlow>();
		int taskNum = 0;
		long scanIndex = 0;
		long unitTotal = jobStoreZkUtils.getSystemTotalSize();
		for (long i = startIndex; ; i++) {
			
			String unitStr = "unit" + i;
			String shardStr = jobStoreZkUtils.locateUnitShardPath(domain, unitStr);
			if (shardStr == null) {
				if (scanIndex >= unitTotal) {
					break;
				}
				continue;
			}
			int state = jobStoreZkUtils.getUnitState(domain, new RTJobId(i));
			scanIndex++;
			if (state == 1) {
				continue;
			}
			if (state == 0) {
//				int initTaskNum = getInitTaskNum(domain, shardStr, unitStr);
				int initTaskNum = getUnitTaskNum(domain, shardStr, unitStr);
				if((taskNum+initTaskNum) > taskLimit ) {
					break;
				}
				if ((taskNum+initTaskNum) == taskLimit) {
					String unitBase = ZslsConstants.ZK_ROOT + "/" + domain + "/" +shardStr+"/"+unitStr+"/"+"unit";
					List<String> units = null;
					try {
						units =  zkclient.getChildren().forPath(unitBase);
					} catch (Exception e) {
						e.printStackTrace();
						return null;
					}
					String unitChild = units.get(0);
					IScheduleUnit iScheduleUnit = JsonSerilizer.deserilize(unitChild, IScheduleUnit.class);
					RTJobFlow scheduleUnit = TaskUtil.parseJobUnit(iScheduleUnit);
					list.add(scheduleUnit);
					break;
				}
				if ((taskNum+initTaskNum) < taskLimit) {
					String unitBase = ZslsConstants.ZK_ROOT + "/" + domain + "/" +shardStr+"/"+unitStr+"/"+"unit";
					List<String> units = null;
					try {
						units =  zkclient.getChildren().forPath(unitBase);
					} catch (Exception e) {
						e.printStackTrace();
						return null;
					}
					String unitChild = units.get(0);
					IScheduleUnit iScheduleUnit = JsonSerilizer.deserilize(unitChild, IScheduleUnit.class);
					RTJobFlow scheduleUnit = TaskUtil.parseJobUnit(iScheduleUnit);
					list.add(scheduleUnit);
					
					taskNum += initTaskNum;
				}
			}
		}
		return list;
	}

	private int getUnitTaskNum(String domain, String shardStr, String unitStr) {
		String unitBase = ZslsConstants.ZK_ROOT + "/" + domain+ "/"+shardStr+"/"+unitStr+"/"+"unit";
		int taskNum = 0;
		try {
			byte[] bytes = zkclient.getData().forPath(unitBase);
			UnitNode unitNode = JsonSerilizer.deserilize(new String(bytes), UnitNode.class);
			taskNum = unitNode.getTaskNum();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return taskNum;
	}
	@SuppressWarnings("unused")
	private int getInitTaskNum(String domain,String shardStr, String unitStr) {
		int initTaskNum = 0;
		String taskBase = ZslsConstants.ZK_ROOT + "/" + domain + "/" + shardStr + "/" + unitStr + "/" + "task";
		try {
			List<String> tasks = zkclient.getChildren().forPath(taskBase);
			for (int i = 0; i < tasks.size(); i++) {
				byte[] bytes = zkclient.getData().forPath(taskBase+"/"+tasks.get(i));
				UserTaskChild userTaskChild = JsonSerilizer.deserilize(new String(bytes), UserTaskChild.class);
				if (userTaskChild.getTaskStat() == TaskStat.Init) {
					initTaskNum++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return initTaskNum;
	}
	@Override
	public RTJobFlow fetchJobBySwiftNum(String domain, String swiftId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<RTJobFlow> fetchUnitsByDate(String domain, Date date, int start, int end) {
		return null;
	}
	private void init() throws Exception {
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		zkclient = ZKOperateHelper.createWithOptions("127.0.0.1:2181",
				ZslsConstants.ZK_NAMESPACE, retryPolicy, 5 * 1000, 3 * 1000);
		zkclient.start();
		zkclient.blockUntilConnected();
	}

	private static String parserTaskIdAndGetUnitIndex(TaskId taskId) {
		int index = taskId.id.lastIndexOf("-");
		String unitId = taskId.id.substring(0, index);
		return unitId;
	}
	private static long parserUnitIdAndGetIndex(String unitId) {
		String prefix = "unit";
		String index = unitId.substring(prefix.length());
		return Long.valueOf(index);
		
	}
	@SuppressWarnings("unused")
	private static String locationShard(long unitSequence) {
		String prefix = "shard";
		long step = IDGenerator.DEFAULT_STEP;
		int index = (int) (unitSequence / step);
		String shard = prefix + index;
		return shard;
	}
	public static long parserShardAndGetIndex(String shardId) {
		String prefix = "shard";
		String indexString = shardId.substring(prefix.length());
		return Long.valueOf(indexString);
	}

	@Override
	public List<ServerQuartzJob> fetchQuartzJobs() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean storeQuartzJob(ServerQuartzJob job, QuartzTrigger trigger, String jobFlow) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addJobFlowInstance(ServerQuartzJob job, Date date) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean updateTaskResult(String jobId, String taskId, Date date, TaskResult ret) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean updateTaskStatsForJob(JobFlow job, Date date) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean updateJobStatus(String jobId, QJobStat stat) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteQuartzJob(String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ServerQuartzJob fetchQuartzJob(String jobId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> fetchRuntimeParams(String jobId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean initForDomain(String domain) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean cleanDomain(String domain) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean updateJob(String domain, RTJobId id, RJobStat stat) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean fetchJobs(String domain, RTJobId id, List<RTJobFlow> result, int taskLimit) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long fetchJobsNum(String domain, Date date) {
		// TODO Auto-generated method stub
		return 0;
	}

}
