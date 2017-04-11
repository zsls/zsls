package org.pbccrc.zsls.tasktracker;

import java.net.InetSocketAddress;

public interface ServerLostListener {
	
	void onServerLost(InetSocketAddress addr);

}
