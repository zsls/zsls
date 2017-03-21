package org.pbccrc.zsls.utils.timeout;

import java.util.HashMap;
import java.util.Map;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.ZslsConstants;
import org.pbccrc.zsls.jobengine.Task;
import org.pbccrc.zsls.service.AbstractService;

public class TimeoutManager extends AbstractService {
	public static final int INIT_QUEUE_SIZE = 1000;
	public static final int MAX_QUEUE_SIZE = 10000;
	
	public TimeoutManager() {
		super(TimeoutManager.class.getSimpleName());
	}

	private TreeSetQueue<ExpireItem> queue;
	private CheckThread thread;
	
	private Map<Integer, TimeoutHandler> handlerMap;
	
	protected void serviceInit(Configuration config) throws Exception {
		queue = new TreeSetQueue<ExpireItem>(new ExpireComparator());
		handlerMap = new HashMap<Integer, TimeoutHandler>();
		thread = new CheckThread();
		super.serviceInit(config);
	}
	
	public void serviceStart() throws Exception {
		thread.start();
		super.serviceStart();
	}
	
	
	public boolean add(Expirable o, int type) {
		ExpireItem item = null;
		try {
			item = new ExpireItem(o, type);
			synchronized (queue) {
				return queue.add(item);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/*
	 * 正常情况不需要调用。因为更高效的做法是可以通过对Expirable置状态，使timeoutCanceled()
	 * 返回true即可。
	 */
	public boolean cancelTimeout(Expirable o) {
		boolean ret = false;
		synchronized (queue) {
			ret = queue.remove(new ExpireItem(o, -1));
		}
		if (!ret) {
			ExpireItem item = thread.getCurrentItem();
			if (item != null && item.item.equals(o)) {
				item.canceled = true;
				ret = true;
			}
		}
		return ret;
	}
	
	public void registerHandler(int type, TimeoutHandler handler) {
		handlerMap.put(type, handler);
	}
	
	private class CheckThread extends Thread {
		volatile ExpireItem item = null;
		public ExpireItem getCurrentItem() {
			return item;
		}
		public void run() {
			while (true) {
				item = queue.take();
				if (item != null && !item.canceled && !item.item.timeoutCanceled()) {
					if (item.type == ZslsConstants.TIMEOUT_TASK) {
						Task t = (Task)item.item;
						if (t.getTimeout() <= 0)
							continue;
					}
					
					long now = System.currentTimeMillis();
					long diff = item.item.expireTime() - now;
					if (diff > 3) {
						try {
							synchronized (queue) {
								// in case new items added into queue during the period of time 
								// between take() and wait() of the queue
								ExpireItem newTop = queue.first();
								if (newTop != null && newTop.item.expireTime() < item.item.expireTime()) {
									queue.add(item);
									item = null;
									continue;
								}
								queue.wait(diff);
							}
						} catch (InterruptedException ignore) {
						}
						diff = item.item.expireTime() - System.currentTimeMillis();
					}
					if (diff > 3) {
						queue.add(item);
						item = null;
						continue;
					}
					else if (!item.canceled && !item.item.timeoutCanceled()) {
						TimeoutHandler handler = handlerMap.get(item.type);
						if (handler != null) {
							try {
								handler.handleTimeout(item.item);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					item = null;
				}
			}
		}
	}

}
