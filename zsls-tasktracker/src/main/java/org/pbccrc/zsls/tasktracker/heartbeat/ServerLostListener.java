package org.pbccrc.zsls.tasktracker.heartbeat;

import java.net.InetSocketAddress;

public interface ServerLostListener {
	
	void onServerLost(InetSocketAddress addr);

}
