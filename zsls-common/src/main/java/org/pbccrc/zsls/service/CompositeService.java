package org.pbccrc.zsls.service;

import java.util.ArrayList;
import java.util.List;

import org.pbccrc.zsls.config.Configuration;

public class CompositeService extends AbstractService {
	
	
	private final List<Service> serviceList = new ArrayList<Service>();

	public CompositeService(String name) {
		super(name);
	}
	
	public List<Service> getServices() {
		return serviceList;
	}
	
	public void addService(Service service) {
		serviceList.add(service);
	}
	
	protected boolean addIfService(Object object) {
		if (object instanceof Service) {
			addService((Service) object);
			return true;
		} else {
			return false;
		}
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		for (Service service : this.serviceList) {
			service.init(conf);
		}
		super.serviceInit(conf);
	}
	
	protected void serviceStart() throws Exception {
		for (Service service : this.serviceList) {
			service.start();
		}
		super.serviceStart();
	}
	
	protected void serviceStop() throws Exception {
		for (Service service : this.serviceList) {
			service.stop();
		}
		super.serviceStop();
	}

}
