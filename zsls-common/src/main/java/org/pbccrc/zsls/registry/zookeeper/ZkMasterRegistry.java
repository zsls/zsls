package org.pbccrc.zsls.registry.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.pbccrc.zsls.config.RegistryConfig;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.exception.ZslsRuntimeException;
import org.pbccrc.zsls.registry.MasterRegistry;
import org.pbccrc.zsls.registry.NotifyEvent;
import org.pbccrc.zsls.registry.NotifyListener;
import org.pbccrc.zsls.registry.RegNode;
import org.pbccrc.zsls.registry.RegistryStateListener;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.JsonSerilizer;
import org.pbccrc.zsls.utils.ZKOperateHelper;

public class ZkMasterRegistry implements MasterRegistry {
	private static final String MASTER_PATH = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_MASTER_SLAVE;
	private static DomainLogger L = DomainLogger.getLogger(ZkMasterRegistry.class.getSimpleName());
	
	private CuratorFramework client;
	private RegistryConfig config;
	private Thread recoverThread;
	
	private PathChildrenCache cache;
	private PathChildrenListener listener;
	
	private List<NotifyListener> notifyListeners;
	private List<RegistryStateListener> stateListeners;
	
	
	public ZkMasterRegistry(RegistryConfig config) throws Exception {
		this.config = config;
		notifyListeners = new ArrayList<NotifyListener>();
		stateListeners = new ArrayList<RegistryStateListener>();
		init();
	}
	
	private class PathChildrenListener implements PathChildrenCacheListener {
		private List<NotifyListener> listeners;
		public PathChildrenListener(List<NotifyListener> listeners) {
			this.listeners = listeners;
		}
		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			switch (event.getType()) {
			case CHILD_ADDED:
				byte[] data = event.getData().getData();
				RegNode node = null;
				try {
					node = JsonSerilizer.deserilize(new String(data), RegNode.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
				NotifyEvent e = NotifyEvent.ADD;
				List<RegNode> list = new ArrayList<RegNode>();
				list.add(node);
				for (NotifyListener l : listeners) {
					l.notify(e, list);
				}
				break;
			case CHILD_REMOVED:
				e = NotifyEvent.REMOVE;
				list = new ArrayList<RegNode>();
				for (NotifyListener l : listeners) {
					l.notify(e, list);
				}
				break;
			default:
				break;
			}
		}
	}

	@Override
	public boolean registerMaster(RegNode node) {
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_MASTER_SLAVE;
		String addr = JsonSerilizer.serilize(node);
		try {
			path = path + "/" + ZslsConstants.ZK_NODE_CURRENT_MASTER;
			client.create().withMode(CreateMode.EPHEMERAL).forPath(path, addr.getBytes());
			return true;
		} catch (NodeExistsException e) {
			RegNode exist = getMaster();
			if (exist != null && exist.equals(node)) {
				try {
					client.inTransaction().delete().forPath(path)
						.and().create().withMode(CreateMode.EPHEMERAL).forPath(path, addr.getBytes())
						.and().commit();
					return true;
				} catch (Exception ee) {
					throw new ZslsRuntimeException(ee);
				}
			}
			return false;
		} catch (Exception e) {
			throw new ZslsRuntimeException(e);
		}
	}

	@Override
	public RegNode getMaster() {
		String path = ZslsConstants.ZK_ROOT + "/" + ZslsConstants.ZK_NODE_MASTER_SLAVE;
		try {
			path = path + "/" + ZslsConstants.ZK_NODE_CURRENT_MASTER;
			if (client.checkExists().forPath(path) != null) {
				byte[] data = client.getData().forPath(path);
				RegNode node = JsonSerilizer.deserilize(new String(data), RegNode.class);
				return node;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void substribe(NotifyListener listener) {
		notifyListeners.add(listener);
	}

	@Override
	public void addServiceListener(RegistryStateListener listener) {
		synchronized (stateListeners) {
			stateListeners.add(listener);
		}
	}

	public void init() throws Exception {
		RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
		client = ZKOperateHelper.createWithOptions(config.getConnAddr(), ZslsConstants.ZK_NAMESPACE, 
				policy, (int)config.getConnTimeout(), (int)config.getSessionTimeout());
		
		client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				switch (newState) {
				case LOST:
					if (recoverThread != null && recoverThread.isAlive()) {
						recoverThread.interrupt();
					}
					try {
						if (cache != null)
							cache.close();
					} catch (Exception ignore) {
					}
					synchronized (stateListeners) {
						for (RegistryStateListener l : stateListeners) {
							l.serviceDisabled();
						}	
					}
					break;
				case RECONNECTED:
					if (recover()) {
						synchronized (stateListeners) {
							for (RegistryStateListener l : stateListeners) {
								l.serviceEnabled();
							}
						}	
					} else {
						recoverThread = new RecoverThread();
						recoverThread.start();
					}
					break;
				case SUSPENDED:
					break;
				default:
					break;
				}
			}
		});
		
		client.start();
		if (!client.blockUntilConnected(5, TimeUnit.SECONDS))
			throw new ZslsRuntimeException("can not connect to zookeeper: " + config.getConnAddr());
		
		listener = new PathChildrenListener(notifyListeners);
		if (client.checkExists().forPath(MASTER_PATH) == null) {
			client.create().creatingParentsIfNeeded().forPath(MASTER_PATH);
		}
		cache = new PathChildrenCache(client, MASTER_PATH, true);
		cache.getListenable().addListener(listener);
		cache.start();
	}
	
	private boolean recover() {
		L.info(ZslsConstants.DEFAULT_DOMAIN, "try recover cache and watchers");
		cache = new PathChildrenCache(client, MASTER_PATH, true);
		cache.getListenable().addListener(listener);
		try {
			cache.start();
		} catch (Exception e) {
			L.error(ZslsConstants.DEFAULT_DOMAIN, "exception when recover cache and watchers: " + e);
			return false;
		}
		return true;
	}
	
	private class RecoverThread extends Thread {
		public void run() {
			while(!recover()) {
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					return;
				}
			}
			synchronized (stateListeners) {
				for (RegistryStateListener l : stateListeners) {
					l.serviceEnabled();
				}			
			}
		}
	}
	
}