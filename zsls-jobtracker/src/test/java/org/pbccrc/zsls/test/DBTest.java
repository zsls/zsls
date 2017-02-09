package org.pbccrc.zsls.test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

public class DBTest {
	
	public static void main(String[] args) throws Exception {
		testDruid();
	}
	
	public static void testDruid() throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		map.put("url", "jdbc:oracle:thin:@115.29.142.149:1521:orcl");
		map.put("username", "hbbtest");
		map.put("password", "123456");
		map.put("initialSize", "1");
		map.put("minIdle", "1");
		map.put("maxActive", "20");
		map.put("maxWait", "6000");
		DruidDataSource ds = (DruidDataSource) DruidDataSourceFactory.createDataSource(map);
		for (int i = 0; i < 5; i++) {
			Connection conn = ds.getConnection();
			System.out.println("active count: " + ds.getActiveCount());
			conn.close();
			System.out.println("active count: " + ds.getActiveCount());
		}
	}

}
