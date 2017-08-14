package org.pbccrc.zsls.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.JobTracker.DomainStatus;
import org.pbccrc.zsls.collection.CopyOnWriteHashMap;
import org.pbccrc.zsls.collection.Pair;
import org.pbccrc.zsls.state.ZslsStateMachine;
import org.pbccrc.zsls.state.ZslsStateMachineFactory;

public class DomainManager {
	
	public static enum DomainType {
		DT(0), 		// delay-time
		RT(1);		// real-time
		int val;
		DomainType(int val) {
			this.val = val;
		}
	}
	
	private static final ZslsStateMachineFactory<DomainStatus> stateFactory =
			new ZslsStateMachineFactory<DomainStatus>(DomainStatus.Prepared)
			.addTransition(DomainStatus.Init, DomainStatus.Running)
			.addTransition(DomainStatus.Init, DomainStatus.Prepared)
			
			.addTransition(DomainStatus.Prepared, DomainStatus.Running)
			
			//.addTransition(DomainStatus.Running, DomainStatus.Abandon)
			.addTransition(DomainStatus.Running, DomainStatus.Stop)
			.addTransition(DomainStatus.Running, DomainStatus.Pause)
			
			.addTransition(DomainStatus.Pause, DomainStatus.Running)
			.addTransition(DomainStatus.Stop, DomainStatus.Running)
			.addTransition(DomainStatus.Stop, DomainStatus.Abandon)
			.build();
	
	private Map<String, Pair<ZslsStateMachine<DomainStatus>, DomainInfo>> domains;
	
	public DomainManager() {
		domains = new CopyOnWriteHashMap<String, Pair<ZslsStateMachine<DomainStatus>, DomainInfo>>();
	}
	
	
	// despite of domain type
	public boolean containsDomain(String domain) {
		return domains.containsKey(domain);
	}
	public void removeDomain(String domain) {
		domains.remove(domain);
	}
	public DomainStatus getDomainStatus(String domain) {
		Pair<ZslsStateMachine<DomainStatus>, DomainInfo> p = domains.get(domain);
		if (p != null)
			return p.getKey().getCurrentState();
		return null;
	}
	public DomainStatus changeDomainStatus(String domain, DomainStatus status) {
		Pair<ZslsStateMachine<DomainStatus>, DomainInfo> p = domains.get(domain);
		if (p != null)
			return p.getKey().doTransition(status);
		return null;
	}
	public DomainType getDomainType(String domain) {
		Pair<ZslsStateMachine<DomainStatus>, DomainInfo> p = domains.get(domain);
		if (p != null)
			return p.getValue().type;
		return null;
	}
	public HashSet<DomainInfo> getAllDomainInfos() {
		HashSet<DomainInfo> set = new HashSet<DomainInfo>();
		for (Pair<ZslsStateMachine<DomainStatus>, DomainInfo> p : domains.values()) {
			DomainInfo info = p.getValue();
			info.status = p.getKey().getCurrentState();
			set.add(info);
		}
		return set;
	}
	public boolean addDomain(String domain, DomainType type) {
		return type == DomainType.RT ? addRTDomain(domain) : addDTDomain(domain);
	}
	
	// RT domains
	public boolean addRTDomain(String domain) {
		synchronized (domains) {
			if (!domains.containsKey(domain)) {
				ZslsStateMachine<DomainStatus> sm = stateFactory.makeStateMachine();
				DomainInfo info = new DomainInfo(domain, DomainType.RT);
				domains.put(domain, 
						new Pair<ZslsStateMachine<DomainStatus>, DomainInfo>(sm, info));
				return true;
			}
		}
		return false;
	}
	
	public List<String> getRTDomains() {
		List<String> list = new ArrayList<String>();
		for (Pair<ZslsStateMachine<DomainStatus>, DomainInfo> p : domains.values()) {
			DomainInfo info = p.getValue();
			if (info.type != DomainType.RT)
				continue;
			list.add(info.name);
		}
		return list;
	}
	public Map<String, DomainInfo> getRTDomainInfos() {
		Map<String, DomainInfo> map = new HashMap<String, DomainInfo>();
		for (Pair<ZslsStateMachine<DomainStatus>, DomainInfo> p : domains.values()) {
			DomainInfo info = p.getValue();
			if (info.type != DomainType.RT)
				continue;
			info.status = p.getKey().getCurrentState();
			map.put(info.name, info);
		}
		return map;
	}
	
	
	// DT domains
	public boolean addDTDomain(String domain) {
		synchronized (domains) {
			if (!domains.containsKey(domain)) {
				ZslsStateMachine<DomainStatus> sm = stateFactory.makeStateMachine();
				DomainInfo info = new DomainInfo(domain, DomainType.DT);
				domains.put(domain, 
						new Pair<ZslsStateMachine<DomainStatus>, DomainInfo>(sm, info));
				return true;
			}
		}
		return false;
	}
	public Map<String, DomainInfo> getDTDomainInfos() {
		Map<String, DomainInfo> map = new HashMap<String, DomainInfo>();
		for (Pair<ZslsStateMachine<DomainStatus>, DomainInfo> p : domains.values()) {
			DomainInfo info = p.getValue();
			if (info.type != DomainType.DT)
				continue;
			info.status = p.getKey().getCurrentState();
			map.put(info.name, info);
		}
		return map;
	}
	public boolean isAllDTDomainsReady() {
		Map<String, DomainInfo> map = getDTDomainInfos();
		for (DomainInfo info : map.values()) {
			if (info.status != DomainStatus.Running)
				return false;
		}
		return true;
	}
	
	public String dumpDomains() {
		Map<String, DomainInfo> dtDomains = getDTDomainInfos();
		Map<String, DomainInfo> rtDomains = getRTDomainInfos();
		StringBuilder b = new StringBuilder();
		b.append("\n");
		b.append("\tRT DOMAINS: -> ");
		for (DomainInfo info : rtDomains.values())
			b.append(info.name).append("(").append(info.status).append(")")
				.append(" | ");
		b.append("\n");
		b.append("\tDT DOMAINS: -> ");
		for (DomainInfo info : dtDomains.values())
			b.append(info.name).append("(").append(info.status).append(")")
				.append(" | ");
		return b.toString();
	}

}
