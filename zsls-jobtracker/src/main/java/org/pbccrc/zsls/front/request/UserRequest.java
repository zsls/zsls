package org.pbccrc.zsls.front.request;

import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.utils.JobUtils;
import org.pbccrc.zsls.api.quartz.CronQuartzTrigger;
import org.pbccrc.zsls.api.quartz.QuartzTrigger;
import org.pbccrc.zsls.api.quartz.SimpleQuartzTrigger;
import org.pbccrc.zsls.entry.NodeId;
import org.pbccrc.zsls.front.request.http.HttpRequestMessage;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.JsonSerilizer;

import io.netty.handler.codec.http.FullHttpRequest;

public class UserRequest {
	public static final String PARAM_TYPE			= "type";
	public static final String PARAM_SUBTYPE		= "subtype";
	public static final String PARAM_QUERY		= "query";
	public static final String PARAM_DOMAIN		= "domain";
	public static final String PARAM_UNITID		= "unitid";
	public static final String PARAM_TIME			= "time";
	public static final String PARAM_TRIGGER		= "trigger";
	public static final String PARAM_JOBTYPE      = "jobtype";
	public static final String PARAM_CMD			= "cmd";
	// scale of fetching data from database
	public static final String PARAM_START		= "start";
	public static final String PARAM_END		= "end";
	
	// for RT domains
	public static final String PARAM_VAL_START	= "startDomain";
	public static final String PARAM_VAL_PAUSE	= "pauseDomain";
	public static final String PARAM_VAL_STOP		= "stopDomain";
	public static final String PARAM_VAL_RESET	= "resetDomain";
	// for DT domains
	public static final String PARAM_VAL_ADDDM	= "addDomain";
	public static final String PARAM_VAL_DELDM 	= "delDomain";
	// for DT job cmd
	public static final String PARAM_VAL_CANCEL	= "cancel";
	public static final String PARAM_VAL_RESUME	= "resume";
	
	protected static DomainLogger L = DomainLogger.getLogger(UserRequest.class.getSimpleName());
	
	/********************* Request Type *********************/
	public static enum QueryType {
		Null(""),
		ScheduleRequest("schedulerequest"),		// schedule request
		SimpleQuartzJob("simplequartzjob"),		// simple quartz job
		CronQuartzJob("cronquartzjob"),			// cron quartz job
		QuartzCmd("quartzcmd"),					// cancel or resume quartz job
		SchedStatQuery("schedulestat"),			// task info of each node for all domains
		SchedCMDQuery("schedulecmd"),			// commands to start or stop a domain
		SchedRedoTask("scheduleredotask"),		// re-do failed task
		SchedMarkTask("schedulemarktaskdone"),	// mark task as finished by force
		DisableNodeQuery("disablenode");		// disable a node
		String name;
		QueryType(String type) {
			this.name = type;
		}
		public String getName() {
			return this.name;
		}
		public static QueryType parseQueryType(String query) {
			if (ScheduleRequest.name.equals(query))
				return QueryType.ScheduleRequest;
			else if (SimpleQuartzJob.name.equals(query))
				return QueryType.SimpleQuartzJob;
			else if (CronQuartzJob.name.equals(query))
				return QueryType.CronQuartzJob;
			else if (SchedStatQuery.name.equals(query))
				return QueryType.SchedStatQuery;
			else if (QuartzCmd.name.equals(query))
				return QueryType.QuartzCmd;
			else if (SchedCMDQuery.name.equals(query))
				return QueryType.SchedCMDQuery;
			else if (SchedRedoTask.name.equals(query))
				return QueryType.SchedRedoTask;
			else if (DisableNodeQuery.name.equals(query))
				return QueryType.DisableNodeQuery;
			else if (SchedMarkTask.name.equals(query))
				return QueryType.SchedMarkTask;
			return QueryType.Null;
		}
	}
	
	
	/*************************** Query Type For ScheduleStatQuery ******************************/
	public static enum SubType {
		Null(""),
		Running("running"),	
		Task("task"),	
		Domain("domain"),
		Swift("swift"),
		Unit("unit");
		
		String name;
		SubType(String type) {
			this.name = type;
		}
		public String getName() {
			return this.name;
		}
		public static SubType parseString(String string) {
			for(SubType st : SubType.values()) {
				if (st.name.equals(string))
					return st;
			}
			return SubType.Null;
		}
	}
	/*************************** JobType ******************************/
	public static enum JobType {
		Null(""),
		DT("dt"),
		RT("rt");
		String name;
		JobType(String name) {
			this.name = name;
		}
		public String getName() {
			return this.name;
		}
		public static JobType parseString(String string) {
			for (JobType jt : JobType.values()) {
				if (jt.name.equals(string))
					return jt;
			}
			return JobType.Null;
		}
	}
	QRequest relatedQRequest;
	public QRequest getRelatedQRequest() {
		return relatedQRequest;
	}
	public void setRelatedQRequest(QRequest relatedQRequest) {
		this.relatedQRequest = relatedQRequest;
	}
	
	QueryType queryType;
	public QueryType getQueryType() {
		return queryType;
	}
	
	
	/* schedule_unit info of normal schedule request */
	IScheduleUnit scheduleInfo;
	public IScheduleUnit getScheduleUnit() {
		return scheduleInfo;
	}
	
	IJobFlow jobFlow;
	public IJobFlow getJobFlow() {
		return jobFlow;
	}
	
	/* quartz_job info of quartz schedule request */
	QuartzTrigger trigger;
	public QuartzTrigger getTrigger() {
		return trigger;
	}
	
	/* other parameters */
	SubType subType;
	public SubType getSubType() {
		return subType;
	}
	String time;
	public String getTime() {
		return time;
	}
	String unitId;
	public String getUnitId() {
		return unitId;
	}
	NodeId node;
	public NodeId getWorkerId() {
		return node;
	}
	String domain;
	public String getDomain() {
		return domain;
	}
	String query;
	public String getQuery() {
		return query;
	}
	JobType jobType;
	public JobType getJobType() {
		return jobType;
	}
	String cmd;
	public String getCmd() {
		return cmd;
	}
	int start;
	public int getStart() {
		return start;
	}
	public void setDefaultStart() {
		if (this.start < 0)
			this.start = 0;
	}
	int end;
	public int getEnd() {
		return end;
	}
	public void setDefaultEnd() {
		this.end = getStart() + 20;
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("received user request -> [");
		builder.append(PARAM_TYPE).append("=").append(this.queryType);
		builder.append(", ").append(PARAM_SUBTYPE).append("=").append(this.subType);
		builder.append(", ").append(PARAM_DOMAIN).append("=").append(this.domain);
		if (this.queryType != QueryType.ScheduleRequest)
			builder.append(", ").append(PARAM_QUERY).append("=").append(this.query);
		builder.append(", ").append(PARAM_CMD).append("=").append(this.cmd);
		builder.append(", ").append(PARAM_TIME).append("=").append(this.time);
		builder.append(", ").append(PARAM_UNITID).append("=").append(this.unitId);
		builder.append(", ").append(PARAM_JOBTYPE).append("=").append(this.jobType);
		builder.append("]");
		return builder.toString();
	}
	
	public static UserRequest parseFromHttpRequest(FullHttpRequest message) {
		HttpRequestMessage httpMessage = new HttpRequestMessage(message);
		UserRequest request = new UserRequest();
		
		request.queryType = QueryType.parseQueryType(httpMessage.getParameter(PARAM_TYPE));
		request.subType = SubType.parseString(httpMessage.getParameter(PARAM_SUBTYPE));
		String jType = httpMessage.getParameter(PARAM_JOBTYPE);
		request.jobType = (jType == null | "".equals(jType)) ? JobType.RT : JobType.parseString(jType);
		request.domain = httpMessage.getParameter(PARAM_DOMAIN);
		request.time = httpMessage.getParameter(PARAM_TIME);
		request.query = httpMessage.getParameter(PARAM_QUERY);
		request.unitId = httpMessage.getParameter(PARAM_UNITID);
		request.cmd = httpMessage.getParameter(PARAM_CMD);
		try {
			if (request.queryType == QueryType.ScheduleRequest) {
				request.scheduleInfo = JsonSerilizer.deserilize(request.query, IScheduleUnit.class);
				request.domain = request.scheduleInfo.domain;
			} 
			else if (request.queryType == QueryType.SimpleQuartzJob) {
				request.trigger = JsonSerilizer.deserilize(httpMessage.getParameter(PARAM_TRIGGER),
						SimpleQuartzTrigger.class);
				request.jobFlow = JobUtils.parseJobFlow(request.query);
			}
			else if (request.queryType == QueryType.CronQuartzJob) {
				request.trigger = JsonSerilizer.deserilize(httpMessage.getParameter(PARAM_TRIGGER),
						CronQuartzTrigger.class);
				request.jobFlow = JobUtils.parseJobFlow(request.query);
			}
			else if (request.queryType == QueryType.DisableNodeQuery) {
				request.node = JsonSerilizer.deserilize(request.getQuery(), NodeId.class);
			}	
			else if (request.queryType == QueryType.SchedStatQuery && request.subType == SubType.Task) {
				try {
				request.start = Integer.valueOf(httpMessage.getParameter(PARAM_START));
				request.end = Integer.valueOf(httpMessage.getParameter(PARAM_END));
				} catch (NumberFormatException ne) {
					request.setDefaultStart();
					request.setDefaultEnd();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return request;
	}

}