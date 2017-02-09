package org.pbccrc.zsls.test;

import org.pbccrc.zsls.front.request.http.HttpRequestMessage;
import org.pbccrc.zsls.front.request.http.HttpResponseMessage;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class HttpTest {
	
	public static void main(String[] args) throws Exception {
		testServer();
	}
	
	public static void testServer() throws Exception {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup(5);
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
        	.channel(NioServerSocketChannel.class)
        	.option(ChannelOption.SO_BACKLOG, 128)
        	.childOption(ChannelOption.SO_KEEPALIVE, true);
        
        b.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
            	ch.pipeline().addLast(new HttpResponseEncoder());
            	ch.pipeline().addLast(new HttpRequestDecoder());
            	ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));	 // 1MB
            	ch.pipeline().addLast(new DemoHandler());
            }
        });
		b.bind(5555).sync();
	}
	
	public static class DemoHandler extends ChannelInboundHandlerAdapter {
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			System.out.println("receive request");
			FullHttpRequest _req = (FullHttpRequest)msg; 
			HttpRequestMessage req= new HttpRequestMessage(_req);
			System.out.println("query type: " + req.getParameter("type"));
			HttpResponseMessage response = new HttpResponseMessage();
			response.setResponseContent("ok from server".getBytes("utf-8"));
			ctx.writeAndFlush(response.getFullResponse());
		}
	}

}
