package org.pbccrc.zsls.front.request.utils;

import org.pbccrc.zsls.front.request.QRequest;
import org.pbccrc.zsls.front.request.QRequest.Status;
import org.pbccrc.zsls.utils.DomainLogger;

public class Replyer {
	private static DomainLogger L = DomainLogger.getLogger(Replyer.class.getSimpleName());
	
	public static void replyAbnormal(QRequest request, Status status, String msg) {
		L.error(request.getUserRequest().getDomain(), msg);
		request.markStatus(status);
		request.setResultInfo(msg);
		replyRequest(request);
	}
	
	public static void replyRequest(QRequest request) {
		request.generateResult();
		request.reply();
	}

}
