package org.pbccrc.zsls.utils.timeout;

import java.util.Comparator;

public class ExpireComparator implements Comparator<ExpireItem> {
	
	@Override
	public int compare(ExpireItem o1, ExpireItem o2) {
		// same object
		if (o1.item == o2.item)
			return 0;
		// expire time
		if (o1.item.expireTime() < o2.item.expireTime())
			return -1;
		else if (o1.item.expireTime() > o2.item.expireTime())
			return 1;
		// id
		else 
			return o1.id.compareTo(o2.id);
	}

}
