package org.pbccrc.zsls.tasks;

import org.pbccrc.zsls.config.Configuration;
import org.pbccrc.zsls.domain.DomainManager.DomainType;
import org.pbccrc.zsls.eventdispatch.AsyncDispatcher;
import org.pbccrc.zsls.eventdispatch.Dispatcher;
import org.pbccrc.zsls.eventdispatch.EventHandler;
import org.pbccrc.zsls.service.CompositeService;

public class HubDispatcher extends CompositeService implements Dispatcher {
//	protected static final DomainLogger L = DomainLogger.getLogger(HubDispatcher.class.getSimpleName());

	private Dispatcher rtDispatcher;
	
	private Dispatcher dtDispatcher;
	
	private HubEventHandler handler;
	
	public HubDispatcher() {
		super(HubDispatcher.class.getSimpleName());
		
		rtDispatcher = new AsyncDispatcher();
		addIfService(rtDispatcher);
		
		dtDispatcher = new AsyncDispatcher();
		addIfService(dtDispatcher);
	}
	
	protected void serviceInit(Configuration conf) throws Exception {
		handler = new HubEventHandler();
		addIfService(handler);
		super.serviceInit(conf);
	}
	
	protected void serviceStart() throws Exception {
		super.serviceStart();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public EventHandler getEventHandler() {
		return handler;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void register(Class<? extends Enum> eventType, EventHandler handler) {
		rtDispatcher.register(eventType, handler);
		dtDispatcher.register(eventType, handler);
	}
	
	
	/**----------------------------------------------**/
	class HubEventHandler implements EventHandler<TaskEvent> {
		@SuppressWarnings("unchecked")
		@Override
		public void handle(TaskEvent event) {
			DomainType type = event.getDomainType();
			Dispatcher dispatcher = type == DomainType.DT ? dtDispatcher : rtDispatcher;
			dispatcher.getEventHandler().handle(event);
		}
	}

}
