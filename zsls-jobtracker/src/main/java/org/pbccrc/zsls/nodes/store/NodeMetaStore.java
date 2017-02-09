package org.pbccrc.zsls.nodes.store;

import java.util.List;
import java.util.Map;

import org.pbccrc.zsls.domain.DomainInfo;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.service.Service;

/**
 * 节点元数据的存储
 */
public interface NodeMetaStore extends Service {
	
	boolean addDomain(String domain, DomainType type);
	
	boolean delDomain(String domain);
	
	boolean storeOrUpdate(NodeMeta meta);
	
	NodeMeta getMeta(String domain, NodeId id);
	
	void removeNode(String domain, NodeId id);
	
	Map<DomainInfo, List<NodeMeta>> getAllNodeMetas();

}
