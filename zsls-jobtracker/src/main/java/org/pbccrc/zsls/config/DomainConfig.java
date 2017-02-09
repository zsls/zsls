package org.pbccrc.zsls.config;

import java.util.ArrayList;
import java.util.List;

public class DomainConfig {
	public static final String DOMAIN = "domain";
	
//	private static final DomainLogger L = DomainLogger.getLogger(DomainConfig.class.getSimpleName());
	
	private List<String> domains;
	
	public List<String> getDomains() {
		return domains;
	}
	
	public static DomainConfig readConfig(Configuration conf) {
		DomainConfig config = new DomainConfig();
		List<String> list = new ArrayList<String>();
		int i = 1;
		String domain = null;
		while ((domain = conf.get(DOMAIN + i++)) != null) {
			domain = domain.trim();
			if (!list.contains(domain))
				list.add(domain);
		}
		config.domains = list;
		return config;
	}
	
	public String toString() {
		String ret = "";
		for (String domain : domains) {
			if (ret.equals(""))
				ret = domain;
			else
				ret = ret + ", " + domain;
		}
		return ret;
	}

}
