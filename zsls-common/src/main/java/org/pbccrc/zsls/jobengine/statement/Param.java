package org.pbccrc.zsls.jobengine.statement;

import java.util.HashMap;
import java.util.Map;

import org.pbccrc.zsls.api.thrift.records.TaskAction;
import org.pbccrc.zsls.entry.TaskResult;
import org.pbccrc.zsls.jobengine.Task.ExecuteResult;

public class Param {
	
	Map<String, String> map;
	
	public Param() {
		map = new HashMap<String, String>();
	}
	
	public void add(String key, String val) {
		map.put(key, val);
	}
	
	public String get(String key) {
		return map.get(key);
	}
	
	public static Param getParam(TaskResult ret) {
		Param p = new Param();
		String code = ret.getAction() == TaskAction.COMPLETE ? ExpObj.KW_VAR_OK : ExpObj.KW_VAR_FAIL;
		p.add(ExpObj.KW_VAR_CODE, code);
		if (ret.getKeyMessage() != null)
			p.add(ExpObj.KW_VAR_MSG, ret.getKeyMessage());
		return p;
	}
	
	public static Param getParam(ExecuteResult result, boolean success) {
		String msg = result != null ? result.keymessage : null;
		return getParam(msg, success);
	}
	
	public static Param getParam(String msg, boolean success) {
		Param p = new Param();
		if (success)
			p.add(ExpObj.KW_VAR_CODE, ExpObj.KW_VAR_OK);
		else
			p.add(ExpObj.KW_VAR_CODE, ExpObj.KW_VAR_FAIL);
		if (msg != null)
			p.add(ExpObj.KW_VAR_MSG, msg);
		return p;
	}

}
