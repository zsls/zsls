package org.pbccrc.zsls.front.request;

import java.io.UnsupportedEncodingException;

import org.pbccrc.zsls.api.client.old.InnerSchedResult;
import org.pbccrc.zsls.api.client.old.InnerSchedResult.ServerStat;
import org.pbccrc.zsls.api.client.old.SchedResult.RetCode;
import org.pbccrc.zsls.front.request.http.HttpResponseMessage;
import org.pbccrc.zsls.ha.HAStatus;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;

import io.netty.handler.codec.http.FullHttpResponse;

public class QRequest extends Replyable<FullHttpResponse>{
	
	private static DomainLogger L = DomainLogger.getLogger(QRequest.class.getSimpleName());
	
	public static enum Status {
		OK,
		Invalid,
		Fail
	}
	
	UserRequest userRequest;
	public UserRequest getUserRequest() {
		return userRequest;
	}
	public void setUserRequest(UserRequest request) {
		this.userRequest = request;
	}
	
	Status status;
	public Status getStatus() {
		return status;
	}
	public boolean inOkStatus() {
		return status == Status.OK;
	}
	public void markStatus(Status stat) {
		this.status = stat;
	}
	
	RTJobFlow unit;
	public RTJobFlow getSchedUnit() {
		return unit;
	}
	public void setSchedUnit(RTJobFlow unit) {
		this.unit = unit;
	}
	
	String resultId;
	public void setResultId(String id) {
		resultId = id;
	}
	
	String resultInfo;
	public void setResultInfo(String info) {
		resultInfo = info;
	}
	public void setResultInfoIfEmpty(String info) {
		if (resultInfo == null)
			this.resultInfo = info;
	}
	
	InnerSchedResult result;
	public InnerSchedResult getResult() {
		return result;
	}
	
	String masterAddress;
	public void setMasterAddress(String masterAddress) {
		this.masterAddress = masterAddress;
	}
	
	HAStatus serverStat;
	public void setServerStatus(HAStatus status) {
		serverStat = status;
	}
	
	public InnerSchedResult generateResult() {
		if (result == null) {
			result = new InnerSchedResult();
			if (status == Status.Fail) 				result.retCode = RetCode.ERROR;
			else if (status == Status.Invalid) 		result.retCode = RetCode.INVALID;	
			else									result.retCode = RetCode.OK;
			
			result.generatedId = resultId;
			result.info = resultInfo;
			
			if (serverStat == HAStatus.MASTER)   	 	result.serverStat = ServerStat.READY;
			else if (serverStat == HAStatus.STANDBY)	result.serverStat = ServerStat.STANDBY;
			else 										result.serverStat = ServerStat.UNKNOWN;
			
			result.masterAddress = masterAddress;
			
			L.info(userRequest.getDomain(), "reply result | " + result.toString());
		}
		return result;
	}

	public void reply() {
		HttpResponseMessage response = new HttpResponseMessage();
		InnerSchedResult result = generateResult();
		String json = ThreadLocalBuffer.getGson().toJson(result);
		try {
			response.setResponseContent(json.getBytes("utf-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		super.reply(response.getFullResponse());
	}

}
