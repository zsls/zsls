package org.pbccrc.zsls.config;

public class WhiteListConfig {
	public static final String KEY_ENABLE		= "whitelist.enable";
	public static final String KEY_FILE		= "whitelist.file";
	public static final String KEY_INTERVAL	= "whitelist.udpate.interval";
	
	public static WhiteListConfig read(Configuration config) {
		WhiteListConfig conf = new WhiteListConfig();
		conf.enabled = config.getBoolean(KEY_ENABLE, true);
		conf.filePath = config.get(KEY_FILE);
		conf.updateInterval = config.getLong(KEY_INTERVAL, 60000L);
		return conf;
	}
	
	private boolean enabled;
	
	private long updateInterval;
	
	private String filePath;

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public long getUpdateInterval() {
		return updateInterval;
	}

	public void setUpdateInterval(int updateInterval) {
		this.updateInterval = updateInterval;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
}
