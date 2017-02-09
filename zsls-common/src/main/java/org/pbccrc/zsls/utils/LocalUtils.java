package org.pbccrc.zsls.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class LocalUtils {
	
	private static String localIp;
	public static String getLocalIp() {
		if (localIp == null) {
			try {
				Enumeration<?> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
				InetAddress ip = null;
				while (allNetInterfaces.hasMoreElements()) {
					NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
					Enumeration<?> addresses = netInterface.getInetAddresses();
					while (addresses.hasMoreElements()) {
						ip = (InetAddress) addresses.nextElement();
						if (ip != null && ip instanceof Inet4Address) {
							if (!ip.getHostAddress().equals("127.0.0.1"))
								localIp = ip.getHostAddress();
						}
					}
				}
			} catch (SocketException e) {
				e.printStackTrace();
			}
		}
		return localIp;
	}
	
	public static InetSocketAddress getAddr(String addr) {
		InetSocketAddress netAddr = null;
		try {
			String ip = addr.split(":")[0];
			int port = Integer.parseInt(addr.split(":")[1]);
			netAddr = new InetSocketAddress(ip, port);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return netAddr;
	}
	
	public static List<InetSocketAddress> getAddrs(String addrs) {
		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
		String[] tmp = addrs.trim().split(";");
		for (String addr : tmp) {
			InetSocketAddress item = getAddr(addr);
			if (item == null)
				throw new IllegalArgumentException("invalid server addresses");
			list.add(item);
		}
		return list;
	}

}
