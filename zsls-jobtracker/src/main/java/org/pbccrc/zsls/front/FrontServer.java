package org.pbccrc.zsls.front;

import org.pbccrc.zsls.config.ServerConfig;
import org.pbccrc.zsls.front.request.FrontServerCallback;
import org.pbccrc.zsls.front.request.Replyable;
import org.pbccrc.zsls.utils.DomainLogger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public abstract class FrontServer<ReqType, ResType> extends ChannelInboundHandlerAdapter {
	
	public static final String SERVER_FRONT	= "front";
	
	protected static DomainLogger L = DomainLogger.getLogger(FrontServer.class.getSimpleName());
	
	protected ServerBootstrap b;
	
	protected ServerConfig config;
	
	protected FrontServerCallback<ReqType, ResType> messageHandler;
	
	protected RequestFactory<ResType> factory;
	
	protected InnerFrontHandler inboundHandler;
	
	public FrontServer(ServerConfig config,
			FrontServerCallback<ReqType, ResType> handler,
			RequestFactory<ResType> factory) {
		this.config = config;
		this.messageHandler = handler;
		this.factory = factory;
		inboundHandler = new InnerFrontHandler();
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(config.getIoThreads());
        b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
        	.channel(NioServerSocketChannel.class)
        	.option(ChannelOption.SO_BACKLOG, 128)
        	.childOption(ChannelOption.SO_KEEPALIVE, true);
        doInitChildHandler();
	}
	
	// to be override
	protected abstract void doInitChildHandler();
	
	public void setMessageHandler(FrontServerCallback<ReqType, ResType> handler) {
		this.messageHandler = handler;
	}
	
	public void start() {
		try {
			b.bind(config.getPort()).sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public int getPort() {
		return config.getPort();
	}
	
	@Sharable
	public class InnerFrontHandler extends ChannelInboundHandlerAdapter {
		@SuppressWarnings("unchecked")
		@Override
	    public void channelRead(ChannelHandlerContext ctx, Object msg)
	            throws Exception {
			if (L.logger().isDebugEnabled())
				L.debug(null, "receive a new request ->");
			Replyable<ResType> request = factory.getRequest();
			request.setChannelContext(ctx);
			messageHandler.callback((ReqType)msg, request);
		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//			L.error(DomainLogger.SYS, "exception caught");
			cause.printStackTrace();
			ctx.close();
		}
		
		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			
		}
	
		@Override
		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
			ctx.close();
		}	
	}
	
	

}
