package org.pbccrc.zsls.api.client.utils;

import org.pbccrc.zsls.api.client.IConvergeGateway;
import org.pbccrc.zsls.api.client.IDataFlow;
import org.pbccrc.zsls.api.client.IDivergeGateway;
import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.ITask;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class JobUtils {
	public static final String JOB		= "job";
	public static final String TASK		= "task";
	public static final String FLOW		= "flow";
	public static final String DIV_GATE	= "div-gateway";
	public static final String CONV_GATE	= "conv-gateway";
	
	public static IJobFlow parseJobFlow(String in) {
		try {
			XStream xstream = new XStream(new DomDriver());
			xstream.alias(JOB, IJobFlow.class);
			xstream.alias(TASK, ITask.class);
			xstream.alias(FLOW, IDataFlow.class);
			xstream.alias(DIV_GATE, IDivergeGateway.class);
			xstream.alias(CONV_GATE, IConvergeGateway.class);
			Object o = xstream.fromXML(in);
			if (o != null) {
				return (IJobFlow)o;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
