package org.pbccrc.zsls.tasktracker.register;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.api.thrift.InnerTrackerProtocol;
import org.pbccrc.zsls.api.thrift.records.RegisterRequest;
import org.pbccrc.zsls.api.thrift.records.RegisterResponse;
import org.pbccrc.zsls.rpc.ZuesRPC;
import org.pbccrc.zsls.tasktracker.config.RuntimeMeta;
import org.pbccrc.zsls.tasktracker.factory.ClientRecordFactory;
import org.pbccrc.zsls.utils.LocalUtils;

public class RegisterManager {
	
	private static Logger L = Logger.getLogger(RegisterManager.class.getSimpleName());
	
	private List<InetSocketAddress> addrs;
	
	private ClientRecordFactory factory;
	
	public RegisterManager(ClientRecordFactory factory, List<InetSocketAddress> list) {
		this.addrs = list;
		this.factory = factory;
	}
	
	private RegisterResult registerTo(InetSocketAddress addr) {
		return registerTo(addr, 0);
	}
	private RegisterResult registerTo(InetSocketAddress addr, int count) {
		RegisterRequest request = factory.getRegisterRequest();
		L.info("now register to server " + addr + ", >> " + request);
		InnerTrackerProtocol.Iface client = null;
		RegisterResponse response = null;
		try {
			client = ZuesRPC.getRpcClient(InnerTrackerProtocol.Iface.class, addr);
			response = client.regiserNode(request);	
		} catch (Exception e) {
			L.error("exception when register to server " + addr + ", " + e);
		} finally {
			ZuesRPC.closeClient(client);
		}
		if (response == null)
			return null;
		
		switch (response.getNodeAction()) {
		
		case INVALID:
			L.error("invalid register response received, detail: " + response.getMessage());
			break;
			
		case NORMAL:
			long heartBeatInterval = response.getHeartBeatInterval();
			long serverTimeout = response.getRegistrySessTimeout();
			RuntimeMeta meta = new RuntimeMeta(heartBeatInterval, serverTimeout);
			L.info("normal register response received " + meta);
			return new RegisterResult(addr, meta);
			
		case NOT_MASTER:
			L.info("not_master register response received");
			String maddr = response.getCluster() != null ? 
					response.getCluster().getMaster() : null;
			if (maddr == null) {
				L.info("no master addr in NotMaster response, probably server disconnected from registry");
				return null;
			}
			InetSocketAddress iaddr = LocalUtils.getAddr(maddr);
			if (iaddr == null) {
				L.error("invalid master addr of register response from server " + addr);
			} else if (iaddr.equals(addr)) {
				L.error("not_master register response's master addr same with current server " + addr);
			} else {
				return registerTo(iaddr, count + 1);
			}
			break;
			
		default:
			break;
		}
		
		return null;
	}
	
	public RegisterResult tryRegister() {
		L.info("Try to register to jobtracker...");
		RegisterResult ret = null;
		for (InetSocketAddress addr : addrs) {
			ret = registerTo(addr);
			if (ret != null)
				return ret;
		}
		return null;
	}

}
