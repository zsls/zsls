package org.pbccrc.zsls.config;

public class DbConfig {
	
	public static final String PRE = "store";
	
	public static DbConfig readConfig(Configuration config) {
		DbConfig conf = new DbConfig();
		conf.setUrl(config.get("store.url"));
		conf.setUser(config.get("store.usr"));
		conf.setPwd(config.get("store.pwd"));
		conf.setInitialSize(config.getInt("store.ds.initialSize"));
		conf.setMaxActive(config.getInt("store.ds.maxActive"));
		conf.setMaxWait(config.getInt("store.ds.maxWait"));
		conf.setMinIdle(config.getInt("store.ds.minIdle"));
		return conf;
	}
	
	private String url;
	
	private String user;
	
	private String pwd;
	
	private int maxActive;
	
	private int maxWait;
	
	private int initialSize;
	
	private int minIdle;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public int getMaxActive() {
		return maxActive;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	public int getMaxWait() {
		return maxWait;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public int getInitialSize() {
		return initialSize;
	}

	public void setInitialSize(int initialSize) {
		this.initialSize = initialSize;
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}
	
}