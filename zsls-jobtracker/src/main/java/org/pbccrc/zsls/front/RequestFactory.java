package org.pbccrc.zsls.front;

import org.pbccrc.zsls.front.request.Replyable;

public interface RequestFactory<ResType> {
	
	Replyable<ResType> getRequest();

}
