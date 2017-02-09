package org.pbccrc.zsls.utils;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

public class ZKOperateHelper {
	public static Logger logger = Logger.getLogger(ZKOperateHelper.class.getName());
	
	
    public static CuratorFramework createSimple(String connectionString)
    {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }

    public static CuratorFramework createWithOptions(String connectionString, String namespace,
    		RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs)
    {
        return CuratorFrameworkFactory.builder().namespace(namespace)
            .connectString(connectionString)
            .retryPolicy(retryPolicy)
            .connectionTimeoutMs(connectionTimeoutMs)
            .sessionTimeoutMs(sessionTimeoutMs)
            .build();
    }
    
    public static CuratorTransactionBridge deleteTree(CuratorFramework client, String path, CuratorTransactionBridge trans) throws Exception {
    	List<String> childs = client.getChildren().forPath(path);
    	if (childs != null) {
	    	for (String child : childs) {
	    		String cpath = path + "/" + child;
	    		trans = deleteTree(client, cpath, trans);
	    	}	
    	}
    	if (trans == null)
    		trans = client.inTransaction().delete().forPath(path);
    	else 
    		trans = trans.and().delete().forPath(path);
    	return trans;
    }
    
}
