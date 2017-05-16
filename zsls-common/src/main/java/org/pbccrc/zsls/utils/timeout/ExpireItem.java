package org.pbccrc.zsls.utils.timeout;

import org.pbccrc.zsls.utils.NumberUtils;

public class ExpireItem implements Comparable<ExpireItem> {
	
	Expirable item;
	int type;
	String id;
	volatile boolean canceled;
	
	public ExpireItem(Expirable item, int type) {
		this.item = item;
		this.type = type;
		// 这里为ExpireItem的id计算MD5值，目的是为了打散id的分布，当大量id具有相同expireTime的时候，
		// 使二叉树尽量的均衡。
		id = item.getUniqueId() != null ? NumberUtils.signMD5(item.getUniqueId()) : "";
	}

	@Override
	public int compareTo(ExpireItem o) {
		// same object
		if (o.item == item)
			return 0;
		// expire time
		if (item.expireTime() < o.item.expireTime())
			return -1;
		else if (item.expireTime() > o.item.expireTime())
			return 1;
		// id
		else 
			return id.compareTo(o.id);
	}
	
	public boolean equals(Object o) {
		if (o == null || !(o instanceof ExpireItem))
			return false;
		if (o == this)
			return true;
		ExpireItem obj = (ExpireItem)o;
		return item.equals(obj.item);
	}
	
	public int hashCode() {
		if (this.item == null)
			return super.hashCode(); 
		return this.item.hashCode();
	}

}
