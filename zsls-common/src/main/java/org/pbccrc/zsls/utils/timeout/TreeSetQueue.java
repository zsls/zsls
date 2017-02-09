package org.pbccrc.zsls.utils.timeout;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

public class TreeSetQueue<E extends Comparable<E>> {
	public static int UNLIMIT_MAX_QUEUE = -1;
	public static int DEFAULT_MAX_QUEUE = 10000;
	
	private TreeSet<E> q;
	
	private int max;
	
	public TreeSetQueue(Comparator<E> comparator) {
		this(UNLIMIT_MAX_QUEUE, comparator);
	}
	
	public TreeSetQueue(int max, Comparator<E> comparator) {
		this.max = max;
		q = new TreeSet<E>(comparator);
	}
	
	public boolean add(E e) {
		if (max != UNLIMIT_MAX_QUEUE && q.size() >= max)
			return false;
		synchronized (this) {
			boolean ret = q.add(e);
			this.notify();
			return ret;	
		}
	}
	
	public synchronized boolean remove(E e) {
		return q.remove(e);
	}
	
	public synchronized E take() {
		if (q.size() == 0) {
			try {
				this.wait();
			} catch (InterruptedException ignore) {
			}
		}
		return q.pollFirst();
	}
	
	public E first() {
		try {
			return q.first();
		} catch (NoSuchElementException e) {
			return null;
		}
	}
	
	public int size() {
		return q.size();
	}
	

}
