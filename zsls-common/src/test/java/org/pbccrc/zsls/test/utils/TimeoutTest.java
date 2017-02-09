package org.pbccrc.zsls.test.utils;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.utils.timeout.Expirable;
import org.pbccrc.zsls.utils.timeout.TimeoutHandler;
import org.pbccrc.zsls.utils.timeout.TimeoutManager;

public class TimeoutTest {
	
	public static void main(String[] args) {
		test();
	}
	
	public static void test() {
		TimeoutManager manager = new TimeoutManager();
		manager.init(new Configuration());
		manager.start();
		manager.registerHandler(0, new TimeoutHandler() {
			@Override
			public void handleTimeout(Expirable item) {
				System.out.println(System.currentTimeMillis() + ", " + item);
			}
		});
		
		Item e1 = new Item(1, 5000);
		Item e2 = new Item(2, 3000);
		Item e3 = new Item(3, 4000);
		
		manager.add(e1, 0);
		manager.add(e2, 0);
		manager.add(e3, 0);
	}
	
	public static class Item implements Expirable {
		
		long timeout;
		int id;
		
		Item(int id, long timeout) {
			this.id = id;
			this.timeout = System.currentTimeMillis() + timeout;
		}
		
		public String toString() {
			return Long.toString(id) + "|" + timeout;
		}

		@Override
		public long expireTime() {
			return timeout;
		}

		@Override
		public boolean timeoutCanceled() {
			return false;
		}

		@Override
		public String getUniqueId() {
			return Integer.toString(id);
		}
		
	}

}
