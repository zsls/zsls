package org.pbccrc.zsls.nodes.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.RegistryConfig;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.domain.DomainInfo;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.service.AbstractService;
import org.pbccrc.zsls.utils.JsonSerilizer;
import org.pbccrc.zsls.utils.ZKOperateHelper;

public class ZKNodeMetaStore extends AbstractService implements NodeMetaStore {
	
	private RegistryConfig config;
	
	private CuratorFramework zkclient;
	
	public ZKNodeMetaStore() {
		super(ZKNodeMetaStore.class.getSimpleName());
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		this.config = RegistryConfig.readConfig(conf);
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		zkclient = ZKOperateHelper.createWithOptions(config.getConnAddr(), ZslsConstants.ZK_NAMESPACE,
				retryPolicy, (int)config.getConnTimeout(), (int)config.getSessionTimeout());
		zkclient.start();
		zkclient.blockUntilConnected();
		// RT domains
		/*DomainConfig domainConfig = DomainConfig.readConfig(conf);
		for (String domain : domainConfig.getDomains()) {
			String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_META + "/" + domain;
			if (zkclient.checkExists().forPath(path) == null) {
				DomainInfo info = new DomainInfo(domain, DomainType.RT);
				byte[] data = JsonSerilizer.serilize(info).getBytes();
				zkclient.create().creatingParentsIfNeeded().forPath(path, data);
			}
		}*/
		// default DT domain
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_META + "/" + ZslsConstants.DEFAULT_DOMAIN;
		byte[] data = null;
		DomainInfo info = null;
		if (zkclient.checkExists().forPath(path) == null) {
			info = new DomainInfo(ZslsConstants.DEFAULT_DOMAIN, DomainType.DT);
			data = JsonSerilizer.serilize(info).getBytes();
			zkclient.create().creatingParentsIfNeeded().forPath(path, data);
		}
	}
	
	public boolean addDomain(String domain, DomainType type) {
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_META + "/" + domain;
		try {
			if (zkclient.checkExists().forPath(path) == null) {
				DomainInfo info = new DomainInfo(domain, type);
				String data = JsonSerilizer.serilize(info);
				zkclient.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
				return true;
			}
		} catch (NodeExistsException e) {
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean delDomain(String domain) {
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_META + "/" + domain;
		try {
			if (zkclient.checkExists().forPath(path) != null) {
				zkclient.delete().deletingChildrenIfNeeded().forPath(path);
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean storeOrUpdate(NodeMeta meta) {
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_META + "/" +
					meta.domain + "/" + meta.nodeId;
		try {
			if (zkclient.checkExists().forPath(path) == null) {
				String data = JsonSerilizer.serilize(meta);
				zkclient.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
			} 
			else {
				String data = JsonSerilizer.serilize(meta);
				zkclient.setData().forPath(path, data.getBytes());
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public NodeMeta getMeta(String domain, NodeId id) {
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_META + "/" +
					domain + "/" + id;
		try {
			byte[] data = zkclient.getData().forPath(path);
			if (data != null) {
				NodeMeta meta = JsonSerilizer.deserilize(new String(data), NodeMeta.class);
				return meta;
			}
		} catch (NoNodeException ignore) {
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Map<DomainInfo, List<NodeMeta>> getAllNodeMetas() {
		Map<DomainInfo, List<NodeMeta>> ret = new HashMap<DomainInfo, List<NodeMeta>>();
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_META;
		try {
			if (zkclient.checkExists().forPath(path) == null) {
				zkclient.create().creatingParentsIfNeeded().forPath(path);
				return ret;
			}
			List<String> domains = zkclient.getChildren().forPath(path);
			for (String domain : domains) {
				List<NodeMeta> list = new ArrayList<NodeMeta>();
				String tmp = path + "/" + domain;
				byte[] data = zkclient.getData().forPath(tmp);
				DomainInfo info = JsonSerilizer.deserilize(new String(data), DomainInfo.class);
				if (info == null) {
					info = domain == ZslsConstants.DEFAULT_DOMAIN ? new DomainInfo(domain, DomainType.DT):
							new DomainInfo(domain, DomainType.RT);
					data = JsonSerilizer.serilize(info).getBytes();
					zkclient.setData().forPath(tmp, data);	
				}
				ret.put(info, list);
				
				List<String> nodes = zkclient.getChildren().forPath(tmp);
				for (String n : nodes) {
					String ntmp = tmp + "/" + n;
					data = zkclient.getData().forPath(ntmp);
					NodeMeta meta = JsonSerilizer.deserilize(new String(data), NodeMeta.class);
					if (meta != null)
						list.add(meta);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public void removeNode(String domain, NodeId id) {
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_META + "/" +
					domain + "/" + id;	
		try {
			zkclient.delete().forPath(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
