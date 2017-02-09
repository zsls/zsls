package org.pbccrc.zsls.utils;

public class ZkPathHelper {
	
	public static String getParentPath(String path) {
		int idx = path.lastIndexOf("/");
		if (idx > 0) {
			return path.substring(0, idx);
		}
		return null;
	}

}
