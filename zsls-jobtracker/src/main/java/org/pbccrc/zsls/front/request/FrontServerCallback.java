package org.pbccrc.zsls.front.request;

public interface FrontServerCallback<ReqType, ResType> {
	
	int callback(ReqType request, Replyable<ResType> reply);

}
