package org.pbccrc.zsls.config;

public class ZslsConstants extends Configuration {
	
	public static final String DISPATCHER_DRAIN_EVENTS_TIMEOUT
			= "dispatcher.drain_events.timeout";
	public static final long DEFAULT_DISPATCHER_DRAIN_EVENTS_TIMEOUT = 300000L; 
	
	/** How long to wait until a node manager is considered dead.*/
	public static final String RM_NM_EXPIRY_INTERVAL_MS = 
			"worknode.liveness.expire.ms";
	public static final int DEFAULT_WN_EXPIRY_INTERVAL_MS = 60000;
	
	
	/** time to wait for all work nodes to re-register when a standby server taking leadership **/
	public static final String LOAD_REGISTER_EXPIRE = "ha.recover.load_timeout";
	public static final int DEFAULT_LOAD_REGISTER_EXPIRE = 20000;
	
	/** heart beat **/
	public static final String HEART_BEAT_INTERVAL = "heartbeat.interval";
	public static final int DEFAULT_HEART_BEAT_INTERVAL = 3000;
	
	/** registry session timeout **/
	public static final long DEFAULT_NODE_LOST	= 60000L;
	
	/** task cache **/
	public static final String TASK_CACHE = "sched.taskcache.size";
	public static final int DEFAULT_TASK_CACHE = 100000;
	public static final int MIN_TASK_CACHE = 10000;
	
	/** default domain **/
	public static final String DEFAULT_DOMAIN = "DOMAIN_DF";
	public static final String FAKE_DOMAIN_DT	= "*DT*";
	
	/** task parameters **/
	public static final String TASKP_SHELL_DIR		= "cmd.dir";
	public static final String TASKP_SHELL_SCRPT		= "cmd.script";
	public static final String TASKP_RETRY_CONDITION	= "retry.condition";
	public static final String TASKP_RETRY_NUM		= "retry.num";
	public static final String TASKP_SLICE_SERIAL    	= "partitionIndex";
	public static final String TASKP_SLICE_Num		= "partitionNum";
	
	/** keyword used to identify important execute result **/
	public static final String TASK_KEY_MSG_PREF		= "%SCHED_MSG{";
	public static final String TASK_KEY_MSG_APPEND	= "}%";
	
	public static final String TASK_RUNTIME_META_PREF		= "%SCHED_META{";
	public static final String TASK_RUNTIME_META_APPEND	= "}%";
	
	/**--------------------- DB related -----------------**/
	public static final String TABLE_NAME_UNIT = "UNITS";
	public static final String TABLE_NAME_TASK = "TASKS";
	public static final String TABLE_NAME_CRON = "CRONS";
	
	
	/**------------------ timeout related ---------------**/
	public static final int TIMEOUT_TASK				= 0;
	public static final int TIMEOUT_LOAD_REGISTER		= 1;
	public static final int TIMEOUT_QUARTZ_JOB		= 2;
	
	
	/**-------------------- ZK related ------------------**/
	public static final String ZK_NAMESPACE = "sys-schedule";
	public static final String ZK_ROOT = "/schedule-v3";
	public static final String ZK_NODE_META = "nodemeta";
	public static final String ZK_NODE_MASTER_SLAVE = "master_root";
	public static final String ZK_NODE_CURRENT_MASTER = "current_master";

	
}
