package org.pbccrc.zsls.front.request.http;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.EndOfDataDecoderException;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;

public class HttpRequestMessage {
	
	private static Charset charset = Charset.forName("utf-8");
	private static HttpDataFactory factory = new DefaultHttpDataFactory(charset);
	
	private FullHttpRequest request;
	
	private Map<String, String> params;
	
	public HttpRequestMessage(FullHttpRequest request) {
		this.request = request;
		params = new HashMap<String, String>();
		if (request.method().equals(HttpMethod.GET)) {
			QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
			Map<String, List<String>> params = decoder.parameters();
			for (String key : params.keySet()) {
				List<String> vallist = params.get(key);
				if (vallist != null && vallist.size() > 0)
					this.params.put(key, vallist.get(0));
			}
		}
		else if (request.method().equals(HttpMethod.POST)) {
			HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(factory, request, charset);
			try {
				while (decoder.hasNext()) {
					InterfaceHttpData data = decoder.next();
					try {
						if (data != null && data.getHttpDataType() == HttpDataType.Attribute) {
							Attribute attr = (Attribute)data;
							params.put(data.getName(), attr.getValue());
						}	
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (data != null)
							data.release();
					}
				}	
			} catch (EndOfDataDecoderException ignore) {
			}
		}
	}
	
	public FullHttpRequest getFullRequest() {
		return request;
	}
	
	public String getParameter(String key) {
		return params.get(key);
	}

}
