package org.pbccrc.zsls.config;

import java.io.IOException;
import java.util.Properties;

public class Configuration {
	public static final String CONFIG_FILE_NAME			= "server.conf";
	public static final int NULL_INT_VAL					= Integer.MIN_VALUE;
	
	public Configuration() {
	}
	
	private Properties properties;
	
	private String filename = CONFIG_FILE_NAME;
	
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public Configuration load() throws IOException {
		properties = new Properties();
		properties.load(getClass().getClassLoader().getResourceAsStream(filename));
		return this;
	} 
	
	public String get(String key) {
		return properties.getProperty(key);
	}
	
	public int getInt(String key) {
		String val = properties.getProperty(key);
		if (val != null) {
			try {
				return Integer.parseInt(val);
			} catch (Exception e) {
			}
		}
		return NULL_INT_VAL;
	}
	public int getInt(String key, int defaultVal) {
		String val = properties.getProperty(key);
		if (val != null) {
			try {
				return Integer.parseInt(val);
			} catch (Exception e) {
			}
		}
		return defaultVal;
	}
	
	public boolean getBoolean(String key, boolean defaultVal) {
		String val = properties.getProperty(key);
		if (val != null) {
			try {
				return Boolean.parseBoolean(val);
			} catch (Exception e) {
			}
		}
		return defaultVal;
	}
	
	public long getLong(String key, long defaultVal) {
		String val = properties.getProperty(key);
		if (val != null) {
			try {
				return Long.parseLong(val);
			} catch (Exception e) {
			}
		}
		return defaultVal;
	}

}
