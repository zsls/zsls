package org.pbccrc.zsls.front.request;

import org.pbccrc.zsls.eventdispatch.AbstractEvent;
import org.pbccrc.zsls.front.request.UserRequest.QueryType;

public class UserRequestEvent extends AbstractEvent<UserRequest.QueryType> {
	
	private QRequest request;

	public UserRequestEvent(QueryType type) {
		super(type);
	}
	
	public UserRequestEvent(QRequest request) {
		super(request.getUserRequest().getQueryType());
		this.request = request;
	}
	
	public QRequest getRequest() {
		return request;
	}

}
