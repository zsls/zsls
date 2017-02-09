package org.pbccrc.zsls.registry;

/**
 * Registry interface.
 * 
 * Since we only utilize the registry to elect master for jobtrackers,
 * a more specific interface MasterRegistry is used in this project.
 *
 * We put Registry here for completion of the registry concept and 
 * maybe for future extension.
 */
public interface Registry {
	
	void register(RegNode node);
	
	void unregister(RegNode node);
	
	
	void substribe(RegNode node, NotifyListener listener);
	
	void unsubstribe(RegNode node, NotifyListener listener);
	
}
