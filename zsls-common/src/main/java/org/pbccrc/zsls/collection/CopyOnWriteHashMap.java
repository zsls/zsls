package org.pbccrc.zsls.collection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CopyOnWriteHashMap<K, V> implements Map<K, V> {
	
	private HashMap<K, V> map;
	
	public CopyOnWriteHashMap() {
		map = new HashMap<K, V>();
	}
	
	public CopyOnWriteHashMap(int capacity) {
		map = new HashMap<K, V>(capacity);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return map.get(key);
	}

	@Override
	public V put(K key, V value) {
		HashMap<K, V> tmp = new HashMap<K, V>(map);
		V v = tmp.put(key, value);
		map = tmp;
		return v;
	}

	@Override
	public V remove(Object key) {
		HashMap<K, V> tmp = new HashMap<K, V>(map);
		V v = tmp.remove(key);
		map = tmp;
		return v;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		HashMap<K, V> tmp = new HashMap<K, V>(map);
		tmp.putAll(m);
		map = tmp;
	}

	@Override
	public void clear() {
		HashMap<K, V> tmp = new HashMap<K, V>(map);
		tmp.clear();
		map = tmp;
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

}
