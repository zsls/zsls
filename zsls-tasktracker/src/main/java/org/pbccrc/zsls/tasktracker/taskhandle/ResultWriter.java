package org.pbccrc.zsls.tasktracker.taskhandle;

import java.util.Map;

public interface ResultWriter {
	
	void writeFeedbackMessage(String msg);
	
	void writeKeyMessage(String msg);
	
	void updateRuntimeParams(Map<String, String> data);

}
