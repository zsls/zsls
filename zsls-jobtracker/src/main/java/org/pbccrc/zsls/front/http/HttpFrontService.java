package org.pbccrc.zsls.front.http;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.config.ServerConfig;
import org.pbccrc.zsls.context.AppContext;
import org.pbccrc.zsls.exception.ServiceStateException;
import org.pbccrc.zsls.front.FrontServer;
import org.pbccrc.zsls.front.RequestFactory;
import org.pbccrc.zsls.front.request.QRequest;
import org.pbccrc.zsls.front.request.Replyable;
import org.pbccrc.zsls.front.request.RequestManager;
import org.pbccrc.zsls.service.CompositeService;
import org.pbccrc.zsls.utils.DomainLogger;

import io.netty.handler.codec.http.FullHttpResponse;

public class HttpFrontService extends CompositeService implements RequestFactory<FullHttpResponse> {
	private static DomainLogger L = DomainLogger.getLogger(HttpFrontService.class.getSimpleName());
	
	private HttpFrontServer server;
	
	private AppContext context;
	
	public HttpFrontService(AppContext context) {
		super(FrontServer.SERVER_FRONT);
		this.context = context;
	}
	
	public HttpFrontServer getServer()	 {
		return server;
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		ServerConfig serverConf = ServerConfig.readConfig(conf, FrontServer.SERVER_FRONT);
		if (serverConf == null)
			throw new ServiceStateException("invalid server configuration");
		
		RequestManager handler = new RequestManager(context);
		addService(handler);
		
		server = new HttpFrontServer(serverConf, handler, this);
		
		super.serviceInit(conf);
	}
	
	protected void serviceStart() throws Exception {
		super.serviceStart();
		server.start();
		L.info(DomainLogger.SYS, "front server started on port " + server.getPort());
	}

	@Override
	public Replyable<FullHttpResponse> getRequest() {
		return new QRequest();
	}

}
