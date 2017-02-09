package org.pbccrc.zsls.front.request.http;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class HttpResponseMessage {
	
	private FullHttpResponse response;
	
	public HttpResponseMessage () {
		
	}
	
	public void setResponseContent(byte[] data) {
		if (response != null) {
			response.release();
		}
		ByteBuf buff = Unpooled.copiedBuffer(data);
		response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buff);
		response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
		response.headers().set(HttpHeaderNames.CONTENT_LENGTH, buff.readableBytes());
	}
	
	public FullHttpResponse getFullResponse() {
		return response;
	}

}
