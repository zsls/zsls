package org.pbccrc.zsls.tasktracker.heartbeat;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.api.thrift.InnerTrackerProtocol;
import org.pbccrc.zsls.api.thrift.records.HeartBeatRequest;
import org.pbccrc.zsls.api.thrift.records.HeartBeatResponse;
import org.pbccrc.zsls.api.thrift.records.NodeAction;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.rpc.ZuesRPC;
import org.pbccrc.zsls.tasktracker.Controller;
import org.pbccrc.zsls.tasktracker.config.RuntimeMeta;
import org.pbccrc.zsls.tasktracker.factory.ClientRecordFactory;

public class HeartBeater extends Thread {
	
	private static Logger L = Logger.getLogger(HeartBeater.class.getSimpleName());
	
	private InnerTrackerProtocol.Iface client;
	private List<ServerLostListener> failListeners;
	private ClientRecordFactory factory;
	private volatile boolean pauseSend;
	private Controller controller;
	
	public HeartBeater() {
		super();
	}
	
	public HeartBeater(ClientRecordFactory factory, Controller controller) {
		super();
		this.setDaemon(true);
		this.heartBeatInterval = ZslsConstants.DEFAULT_HEART_BEAT_INTERVAL;
		this.failTimeout = ZslsConstants.DEFAULT_NODE_LOST;
		this.factory = factory;
		this.controller = controller;
		pauseSend = true;
		failListeners = new ArrayList<ServerLostListener>();
	}
	
	
	// heart beat interval
	private volatile long heartBeatInterval;
	public long getHeartBeatInterval() {
		return heartBeatInterval;
	}
	public void setHeartBeatInterval(long heartBeatInterval) {
		this.heartBeatInterval = heartBeatInterval;
	}
	
	
	// server timeout
	private volatile long failTimeout;
	public long getFailTimeout() {
		return failTimeout;
	}
	public void setFailTimeout(long failTimeout) {
		this.failTimeout = failTimeout;
	}


	// server addr
	private InetSocketAddress addr;
	public InetSocketAddress getAddr() {
		return addr;
	}
	public void updateServerAddr(InetSocketAddress addr) {
		invalidAndCloseClient();
		this.addr = addr;
		pauseSend = false;
	}
	
	
	// control
	public void pauseSend() {
		pauseSend = true;
	}
	public void updateRuntimeMeta(RuntimeMeta meta) {
		this.heartBeatInterval = meta.heartBeatInterval;
		this.failTimeout = meta.serverRegistryTimeout;
	}
	
	
	// alive signal
	private volatile boolean stop;
	public void stopBeat() {
		stop = true;
	}
	
	
	public void registerFailListener(ServerLostListener listener) {
		failListeners.add(listener);
	}
	
	private void invalidAndCloseClient() {
		if (client != null) {
			InnerTrackerProtocol.Iface cli = client;
			client = null;
			ZuesRPC.closeClient(cli);	
		}
	}
	
	private HeartBeatResponse heartbeatOnce(HeartBeatRequest request) throws Exception {
		HeartBeatResponse response = null;
		if (client != null) {
			try {
				response = client.heartBeat(request);
				return response;
			} catch (Exception e) {
				invalidAndCloseClient();
			}
		}
		try {
			client = ZuesRPC.getRpcClient(InnerTrackerProtocol.Iface.class, addr);
			response = client.heartBeat(request);
			return response;
		} catch (Exception e) {
			invalidAndCloseClient();
			throw e;
		}
	}
	
	public void run() {
		long fail = 0;
		while (!stop) {
			if (addr != null && !pauseSend) {
				HeartBeatRequest request = factory.getHeartBeat();
				HeartBeatResponse response = null;
				try {
					response = heartbeatOnce(request);
				} catch (Exception e) {
					L.error("failed to heart beat to server " + addr + ", " + e);
					fail += heartBeatInterval;
				}
				if (response != null) {
					L.debug("receive heart beat response from server " + addr);
					fail = 0L;		
					if (response.getNodeAction() != NodeAction.NORMAL) {
						L.error(response.getNodeAction() + " heart beat response received, would reregister");
						controller.pauseSystemAndReRegister();
					}
				}
				
				// server address is expected to be updated by listener
				if (fail >= failTimeout) {
					for (ServerLostListener l : failListeners)
						l.onServerLost(addr);
					fail = 0L;
				}	
			}
			
			try {
				Thread.sleep(heartBeatInterval);
			} catch (InterruptedException ignore) {
			}
		}
		L.warn("heartbeater stopped");
	}
	
}
