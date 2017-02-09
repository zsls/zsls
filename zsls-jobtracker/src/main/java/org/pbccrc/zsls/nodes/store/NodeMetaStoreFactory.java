package org.pbccrc.zsls.nodes.store;

public class NodeMetaStoreFactory {
	
	public static NodeMetaStore getMetaStore() {
		return new ZKNodeMetaStore();
	}

}
