package org.pbccrc.zsls.sched;

public class NodePickerFactory {
	
	public static NodePicker getNodePicker() {
		return new NormNodePicker();
	}

}
