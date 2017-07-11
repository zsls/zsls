package org.pbccrc.zsls.jobstore.zookeeper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.utils.JsonSerilizer;

public class JobStoreZkUtils {

	//k/v domain-map<u,t>
	//u/t shardid-shardNode
	private HashMap<String, HashMap<String, ShardNode>> domainshardIdShardNodeCache; //kv -- domain/map
//	private String maxUnitId;
	
	//k/v domain-map<u,t>
	//u/t unitid-ShardNode
	private HashMap<String, HashMap<String, ShardNode>> domainUnitIdShardNodeCache;
	private int shardCapacity = 3000;
	private static JobStoreZkUtils instance;

	private CuratorFramework zkClient;
	private JobStoreZkUtils(CuratorFramework zkClient) {
		this.zkClient = zkClient;
		domainshardIdShardNodeCache = new HashMap<String, HashMap<String,ShardNode>>();
		domainUnitIdShardNodeCache = new HashMap<String, HashMap<String,ShardNode>>();
		init();
	}
	
	public static JobStoreZkUtils getInstance(CuratorFramework zkClient) {
		
		if (instance == null) {
			synchronized(JobStoreZkUtils.class) {
				if (instance == null) {
					instance = new JobStoreZkUtils(zkClient);
				}
			}
		}
		return instance;
	}
	public long getSystemTotalSize() {
		long total = 0;
		Set<String> domainSet = domainUnitIdShardNodeCache.keySet();
		for (String domain : domainSet) {
			total+= domainUnitIdShardNodeCache.get(domain).size();
		}
		return total;
	}
	public void addUnit(String domain, String shard, RTJobId unitId, ScheduleUnitNode scheduleUnitNode) {
		
		HashMap<String, ShardNode> shardIdShardNodes = domainshardIdShardNodeCache.get(domain);
		if (shardIdShardNodes == null) {
			shardIdShardNodes = domainshardIdShardNodeCache.put(domain, new HashMap<String, ShardNode>());
		}
		ShardNode shardNode = shardIdShardNodes.get(shard);
		if (shardNode == null) {
			shardIdShardNodes.put(shard, new ShardNode());
			shardNode = shardIdShardNodes.get(shard);
		}
		HashMap<String, ShardNode> unitIdShardNodes = domainUnitIdShardNodeCache.get(domain);
		if (unitIdShardNodes == null) {
			domainUnitIdShardNodeCache.put(domain, new HashMap<String, ShardNode>());
			unitIdShardNodes = domainUnitIdShardNodeCache.get(domain);
		}
		shardNode.incUnitSize();
		shardNode.setMaxUnitID(unitId.getId());
		shardNode.setMinUnitID(unitId.getId());
		shardNode.getShardChildren().put(unitId.toString(), scheduleUnitNode);
		unitIdShardNodes.put(unitId.toString(), shardNode);
	}
	public String locateUnitShardPath(String domain, RTJobId unitID) {
		
		HashMap<String, ShardNode> unitShard = domainUnitIdShardNodeCache.get(domain);
		if (unitShard == null) {
			return null;
		}
		ShardNode shardNode = unitShard.get(unitID.toString());
		if (shardNode == null) {
			return null;
		}
		String shard = shardNode.toString();
		return shard ;
	}
	public String locateUnitShardPath(String domain, String unitNodeStr) {
		HashMap<String, ShardNode> unitShard = domainUnitIdShardNodeCache.get(domain);
		if (unitShard == null) {
			return null;
		}
		ShardNode shardNode = unitShard.get(unitNodeStr);
		if (shardNode == null) {
			return null;
		}
		String shard = shardNode.toString();
		return shard ;
	}
	public void updateShardNode1() {}
	public void updateShardNode2() {}
	public void updateShardNode3() {}
	public synchronized String getDomainAvailableShard(String domain) {
		
		HashMap<String, ShardNode> shardIdNodes = domainshardIdShardNodeCache.get(domain); //kv / shardname-shard
		if (shardIdNodes == null) {
			shardIdNodes = new HashMap<String, ShardNode>();
			shardIdNodes.put("shard0", new ShardNode(0));
			domainshardIdShardNodeCache.put(domain, shardIdNodes);
			return "shard0";
		}
		Iterator<Map.Entry<String, ShardNode>> it = shardIdNodes.entrySet().iterator();
		ShardNode availableShardNode = null;
		while (it.hasNext()) {
			Map.Entry<String, ShardNode> entry = it.next();
			availableShardNode = entry.getValue();
			if(availableShardNode.getUnitSize() < shardCapacity) {
//				availableShardNode.setId(k);
				break;
			} else {
				availableShardNode = null;
			}
		}
		if (availableShardNode == null) {
			//需要创建新的shard
			for (int i = 0; ; i++) {
				String shardKey = "shard" + i;
				 availableShardNode = shardIdNodes.get(shardKey);
				if (availableShardNode == null) {
					shardIdNodes.put(shardKey, new ShardNode(i));
					availableShardNode = shardIdNodes.get(shardKey);
					availableShardNode.setUnitSize(0);
					break;
				}
			}
		}
		return availableShardNode.toString();
	}

	/*
	 * unitstate : 1.finished 2.failed
	 */
	public boolean markUnitState(String domain, RTJobId id, int unitState) {
		
		HashMap<String, ShardNode> unitIdShardNode = domainUnitIdShardNodeCache.get(domain);
		if (unitIdShardNode == null) {
			return false;
		}
		ShardNode shardNode = unitIdShardNode.get(id.toString());
		ScheduleUnitNode unitNode = shardNode.getShardChildren().get(id.toString());
		if (unitNode == null) {
			return false;
		}
		unitNode.setUnitState(unitState);
		return true;
	}
	public int getUnitState(String domain, RTJobId unitID) {
		HashMap<String, ShardNode> unitIdShardNode = domainUnitIdShardNodeCache.get(domain);
		if (unitIdShardNode == null) {
			return -1;
		}
		ShardNode shardNode = unitIdShardNode.get(unitID.toString());
		if (shardNode == null) {
			return -1;
		}
		ScheduleUnitNode unitNode = shardNode.getShardChildren().get(unitID.toString());
		if (unitNode == null) {
			return -1;
		}
		return unitNode.getUnitState();
	}
	public long getMaxIdUnit(String domain) {
		HashMap<String, ShardNode> shardIdShardNodes = domainshardIdShardNodeCache.get(domain);
		Iterator<Map.Entry<String, ShardNode>> it = shardIdShardNodes.entrySet().iterator();
		long maxUnitID = -1;
		while (it.hasNext()) {
			Map.Entry<String, ShardNode> entry = it.next();
			ShardNode shardNode = entry.getValue();
			long currentMax = shardNode.getMaxUnitID();
			if (currentMax > maxUnitID) {
				maxUnitID = currentMax;
			}
		}
		return maxUnitID;
	}

	private void init() {
		String zslsBase = ZslsConstants.ZK_ROOT;
		if (zkClient == null) {
			throw new IllegalArgumentException("argument error ...");
		}
		//初始化内存
		//1.得到各个domain
		//得到了所有的domain 和 domain下的shard
		try {
			List<String> domains = zkClient.getChildren().forPath(zslsBase);
			for (int i = 0; i < domains.size(); i++) {
				String domain = domains.get(i);
				domainUnitIdShardNodeCache.put(domain, new HashMap<String, ShardNode>());
				String domainBase = zslsBase + "/" + domain;
				List<String> shards = zkClient.getChildren().forPath(domainBase);
				for (int j = 0; j < shards.size(); j++) {
					String shard = shards.get(j);
//					byte[] datas = zkClient.getData().forPath(shardBase);
//					ShardNode shardNode = JsonSerilizer.deserilize(new String(datas), ShardNode.class);
					ShardNode shardNode = new ShardNode(ZookeeperJobStore.parserShardAndGetIndex(shard));
					HashMap<String, ShardNode> shardIdNodeMap = domainshardIdShardNodeCache.get(domain);
					if (shardIdNodeMap == null) {
						 domainshardIdShardNodeCache.put(domain, new HashMap<String, ShardNode>());
						shardIdNodeMap = domainshardIdShardNodeCache.get(domain);
					}
					shardIdNodeMap.put(shard, shardNode);
				}
			}
			//2.以上代码读取了到shard层的数据
			//接下来读取scheduleunit层的数据
			Iterator<Map.Entry<String, HashMap<String, ShardNode>>> it = domainUnitIdShardNodeCache.entrySet().iterator(); 
			while (it.hasNext()) {
				
				Map.Entry<String, HashMap<String, ShardNode>> entry = it.next();
				Iterator<Map.Entry<String, ShardNode>> iterator = entry.getValue().entrySet().iterator();
				String domain = entry.getKey();
				while (iterator.hasNext()) {
					Map.Entry<String, ShardNode> e = iterator.next();
					String shardId = e.getKey();
					String shardBase = zslsBase + "/" + domain+"/"+shardId;
					ShardNode shardNode = e.getValue();
					
					List<String> units = zkClient.getChildren().forPath(shardBase);
					for (int i = 0; i < units.size(); i++) {
						byte[] bytes = zkClient.getData().forPath(shardBase+"/"+units.get(i));
						ScheduleUnitNode unitNode = JsonSerilizer.deserilize(new String(bytes), ScheduleUnitNode.class);
						domainUnitIdShardNodeCache.get(domain).put(units.get(i), shardNode);
						shardNode.getShardChildren().put(units.get(i), unitNode);
						shardNode.incUnitSize();
						shardNode.setMaxUnitID(unitNode.getId());
						shardNode.setMinUnitID(unitNode.getId());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
