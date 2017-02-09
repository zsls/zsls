package org.pbccrc.zsls.utils;

public class JsonSerilizer {
	
	public static String serilize(Object obj) {
		try {
			return ThreadLocalBuffer.getGson().toJson(obj);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static <T> T deserilize(String json, Class<T> t) {
		try {
			return (T) ThreadLocalBuffer.getGson().fromJson(json, t);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
