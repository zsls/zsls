package org.pbccrc.zsls.front.request;

import io.netty.channel.ChannelHandlerContext;

public abstract class Replyable<ResType> {
	
	ChannelHandlerContext cxt;
	
	public void setChannelContext(ChannelHandlerContext cxt) {
		this.cxt = cxt;
	}
	
	public void reply(ResType response) {
		cxt.writeAndFlush(response);
	}

}
