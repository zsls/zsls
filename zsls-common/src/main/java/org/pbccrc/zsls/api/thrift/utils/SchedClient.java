package org.pbccrc.zsls.api.thrift.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.log4j.Logger;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.old.InnerSchedResult;
import org.pbccrc.zsls.api.client.old.InnerSchedResult.ServerStat;
import org.pbccrc.zsls.api.client.old.SchedResult;
import org.pbccrc.zsls.api.client.old.SchedResult.RetCode;
import org.pbccrc.zsls.api.quartz.CronQuartzTrigger;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.api.quartz.SimpleQuartzTrigger;
import org.pbccrc.zsls.exception.ZslsRuntimeException;
import org.pbccrc.zsls.utils.JsonSerilizer;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;

import com.google.gson.Gson;

public class SchedClient {
	private static final Logger LOGGER = Logger.getLogger(SchedClient.class);
	
	public static final String CONTENT_TYPE_KEY	= "Content-Type";
	public static final String CONTENT_TYPE_VAL	= "application/x-www-form-urlencoded;charset=UTF-8";
	
	private final static int DEFAULT_TIMEOUT = 3000;
	
	private final static String SCHED_TYPE = "type";
	
	private final static String SCHED_TYPE_REQUEST			= "schedulerequest";
	private final static String SCHED_TYPE_QUARTZ_SIMPLE		= "simplequartzjob";
	private final static String SCHED_TYPE_QUARTZ_CRON		= "cronquartzjob";
	private final static String SCHED_TYPE_STAT				= "schedulestat";
	
	private final static String SCHED_SUBTYPE = "subtype";
	private final static String SCHED_SUBTYPE_SWIFTNUM = "swift";
	private final static String SCHED_SUBTYPE_RUNNING  = "running";
	
	private final static String SCHED_JOBTYPE = "jobtype";
	private final static String SCHED_QUERY = "query";
	private final static String SCHED_TRIGGER= "trigger";
	private final static String SCHED_DOMAIN = "domain";
	
	private final static String SCHED_JOBTYPE_DT = "dt";
	
	private List<String> addresses;
	private int readTimeout;
	
	// local cache of master address
	private static String masterAddr;
	
	/* IOException when receiving response */
	public static class RecvIOException extends IOException {
		private static final long serialVersionUID = -2995885445569164986L;
		public RecvIOException(String msg) {
			super(msg);
		}
	}
	
	/***** constructors *****/
	public SchedClient(List<String> addrList, long timeout) {
		this.addresses = addrList;
		this.readTimeout = (int)timeout;
	}
	public SchedClient(String addr, long timeout) {
		 this(Arrays.asList(addr.split(";")), timeout);		
	}
	public SchedClient(List<String> addrList) {
		this(addrList, DEFAULT_TIMEOUT);
	}
	public SchedClient(String addr) {
		this(Arrays.asList(addr.split(";")), DEFAULT_TIMEOUT);
	}
	public SchedClient(String ip, int port) {
		this(Arrays.asList(new String[]{ip + ":" + port}));
	}
	
	
	private HttpConnection connect(String addr) throws IOException {
		HttpConnection conn = null;
		try {
			conn = new HttpConnection(addr.split(":")[0], 
					Integer.parseInt(addr.split(":")[1]));
		} catch (Exception e) {
			throw new IOException("Invalid Addr: " + addr);
		}
	    HttpConnectionParams params = new HttpConnectionParams();
	    params.setConnectionTimeout(readTimeout / 2);
	    params.setSoTimeout(readTimeout);
	    conn.setParams(params);
	    conn.open();
		return conn;
	}
	private HttpConnection tryConnect(String addr) {
		try {
			return connect(addr);
		} catch (IOException e) {
			LOGGER.error("failed to connecto to " + addr + ", error: " + e.getMessage());
			return null;
		}
	}
	
	private InnerSchedResult query(HttpConnection conn, Map<String, String> params, boolean isGet) throws IOException{
		LOGGER.debug("send request to " + conn.getHost() + ":" + conn.getPort());
		Gson gson = ThreadLocalBuffer.getGson();
		HttpMethodBase method = null;
		if (isGet) {
			method = new GetMethod();
			StringBuffer content = new StringBuffer();
			for (Map.Entry<String, String> entry : params.entrySet()) {
				content.append(entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), "utf-8") + "&");
			}
			method.setQueryString(content.substring(0, content.length() - 1));
		}
		else {
			method = new PostMethod();
			method.setRequestHeader(CONTENT_TYPE_KEY, CONTENT_TYPE_VAL);
			Iterator<Map.Entry<String, String>> it = params.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				String key = entry.getKey();
				String value = entry.getValue();
				((PostMethod)method).addParameter(key, value);
			}
		}
		InnerSchedResult ret = null;
		
        InputStream in = null;
        BufferedReader br = null;
		try {
	        method.execute(new HttpState(), conn);
	        
			in = method.getResponseBodyAsStream();
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			StringBuilder result = new StringBuilder();
			while ((line = br.readLine()) != null) {
				result.append(line);
			}
			String cnt = result.toString();
			ret = gson.fromJson(cnt, InnerSchedResult.class);
			if (ret == null)
				throw new IOException("Invalid Result received: " + cnt);
			return ret;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RecvIOException(e.getMessage());
		} finally {
			try {
				br.close();
				in.close();
				conn.close();
			} catch (Exception e) {	
			}
		}
	}
	
	public SchedResult sendRequest(Map<String, String> params, boolean isGetMethod) throws IOException {
		// connect
		HttpConnection conn = null; 
		if (masterAddr != null) {
			conn = tryConnect(masterAddr);
			if (conn == null) masterAddr = null;
		}
		if (conn == null) {
			for (int i = 0; conn == null && i < addresses.size() && !addresses.get(i).equals(masterAddr); i++)
				conn = tryConnect(addresses.get(i));
		}
		if (conn == null)
			throw new IOException("cannnot connect to servers, all servers not available");
		
		// send request
		InnerSchedResult ret = query(conn, params, isGetMethod);
		
		// if not master, try connect to master and send
		if (ret.retCode == RetCode.ERROR && 
			ret.serverStat != ServerStat.READY &&
			ret.masterAddress != null) 
		{
			masterAddr = ret.masterAddress;
			conn = tryConnect(masterAddr);
			if (conn == null) {
				masterAddr = null;
				throw new IOException("cannot connect to informed master: " + masterAddr);
			}
			ret = query(conn, params, isGetMethod);
		} 
		// the address happen to be the master, update the cache.
		else if (ret.retCode == RetCode.OK && masterAddr == null) {
			masterAddr = conn.getHost() + ":" + conn.getPort();
		}
		
		SchedResult result = new SchedResult(ret);
		return result;
	}
	
	
	// ------------------------------------------ //
	
	static class RunningUnit {
		public List<String> units;
	}
	
	
	/************************** public interfaces **************************/
	
	public SchedResult send2Schedule(IScheduleUnit unit) throws IOException {
		Gson gson = ThreadLocalBuffer.getGson();
		Map<String, String> params = new HashMap<String, String>();
		params.put(SCHED_TYPE, SCHED_TYPE_REQUEST);
		params.put(SCHED_QUERY, gson.toJson(unit));
		return sendRequest(params, false);
	}
	
	public SchedResult send2Schedule(QuartzTrigger trigger, String job) throws IOException {
		Gson gson = ThreadLocalBuffer.getGson();
		Map<String, String> params = new HashMap<String, String>();
		if (trigger instanceof SimpleQuartzTrigger)
			params.put(SCHED_TYPE, SCHED_TYPE_QUARTZ_SIMPLE);
		else if (trigger instanceof CronQuartzTrigger)
			params.put(SCHED_TYPE, SCHED_TYPE_QUARTZ_CRON);
		params.put(SCHED_TRIGGER, gson.toJson(trigger));
		params.put(SCHED_QUERY, job);
		params.put(SCHED_JOBTYPE, SCHED_JOBTYPE_DT);
		return sendRequest(params, false);
	}
	
	public SchedResult checkUnitExist(String domain, String swiftNum) throws IOException {
		Map<String, String> params = new HashMap<String, String>();
		params.put(SCHED_TYPE, SCHED_TYPE_STAT);
		params.put(SCHED_SUBTYPE, SCHED_SUBTYPE_SWIFTNUM);
		params.put(SCHED_DOMAIN, domain);
		params.put(SCHED_QUERY, swiftNum);
		return sendRequest(params, true);
	}
	
	public int getRunningUnitNum(String domain) throws IOException {
		Map<String, String> params = new HashMap<String, String>();
		params.put(SCHED_TYPE, SCHED_TYPE_STAT);
		params.put(SCHED_SUBTYPE, SCHED_SUBTYPE_RUNNING);
		params.put(SCHED_DOMAIN, domain);
		SchedResult ret = sendRequest(params, true);
		if (ret.retCode == RetCode.OK && ret.info != null) {
			RunningUnit u = JsonSerilizer.deserilize(ret.info, RunningUnit.class);
			if (u != null)
				return u.units.size();
		}
		throw new ZslsRuntimeException("can not get running unit number: " + ret);
	}
	
}
