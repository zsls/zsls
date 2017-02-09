package org.pbccrc.zsls.front.http;

import org.pbccrc.zsls.config.ServerConfig;
import org.pbccrc.zsls.front.FrontServer;
import org.pbccrc.zsls.front.RequestFactory;
import org.pbccrc.zsls.front.request.FrontServerCallback;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class HttpFrontServer extends FrontServer<FullHttpRequest, FullHttpResponse> {
	
	public HttpFrontServer(ServerConfig config, 
			FrontServerCallback<FullHttpRequest, FullHttpResponse> handler,
			RequestFactory<FullHttpResponse> factory) {
		super(config, handler, factory);
	}

	protected void doInitChildHandler() {
		b.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
            	ch.pipeline().addLast(new HttpResponseEncoder());
            	ch.pipeline().addLast(new HttpRequestDecoder());
            	ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));	 // 1MB
            	ch.pipeline().addLast(inboundHandler);
            }
        });
	}

}
