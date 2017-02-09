package org.pbccrc.zsls.front.result;

import java.util.HashMap;
import java.util.Map;

public class ResultNode extends Result {
	public static final String DOMAIN		= "domain";
	public static final String TIME		= "createtime";
	public static final String STAT 		= "stat";
	public static final String TASK_ID	= "taskId";
	public static final String UNITS		= "units";
	public static final String LAST_EXETIME = "lastExetime";
	public static final String EXETIME = "exetime";
	public static final String EXPRESSION = "expression";
	public static final String RESULT_MESSAGE = "resultMessage";
	public static final String PARAMETERS = "parameters";
	
	private Map<String, Object> map;
	
	public ResultNode() {
		map = new HashMap<String, Object>();
	}
	
	public Map<String, Object> data() {
		return map;
	}
	
	public void setDomain(String domain) {
		map.put(DOMAIN, domain);
	}
	
	public String getDomain() {
		Object o = map.get(DOMAIN);
		return o == null ? null : o.toString();
	}
	
	public String getCreatetime() {
		Object o = map.get(TIME);
		return o == null ? null : o.toString();
	}
	
	public void setCreatetime(String createtime) {
		map.put(TIME, createtime);
	}
	
	public void setStatus(String status) {
		map.put(STAT, status);
	}
	
	public String getStatus() {
		Object o = map.get(STAT);
		return o == null ? null : o.toString();
	}
	
	public void setLastExeTime(String time) {
		map.put(LAST_EXETIME, time);
	}
	
	public String getLastExeTime() {
		Object o = map.get(LAST_EXETIME);
		return o == null ? null : o.toString();
	}
	
	public void setExpression(String expression) {
		map.put(EXPRESSION, expression);
	}
	
	public String getExpression() {
		Object o = map.get(EXPRESSION);
		return o == null ? null : o.toString();
	}
	
	public void setExeTime(String exetime) {
		map.put(EXETIME, exetime);
	}
	
	public String getExeTime() {
		Object o = map.get(EXETIME);
		return o == null? null : o.toString();
	}
	
	public void setResultMsg(String message) {
		map.put(RESULT_MESSAGE, message);
	}
	
	public String getResultMsg() {
		Object o = map.get(RESULT_MESSAGE);
		return o == null ? null : o.toString();
	}
	
	public void addChild(String key, Result val) {
		map.put(key, val.data());
	}
	
	public void addParam(String key, Object val) {
		map.put(key, val);
	}
	
}
