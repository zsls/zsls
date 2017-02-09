package org.pbccrc.zsls.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class DateUtils {
	private static SimpleDateFormat sdf_judge = new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat sdf_store = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
	private static SimpleDateFormat sdf_timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static boolean areSameDay(Date dateA, Date dateB) {
		Calendar calDateA = Calendar.getInstance();
		calDateA.setTime(dateA);
		
		Calendar calDateB = Calendar.getInstance();
		calDateB.setTime(dateB);
		
		return calDateA.get(Calendar.YEAR) == calDateB.get(Calendar.YEAR)
				&& calDateA.get(Calendar.MONTH) == calDateB.get(Calendar.MONTH)
				&& calDateA.get(Calendar.DATE) == calDateB.get(Calendar.DATE);
	}
	
	public static Date getDate(String date) throws ParseException {
		return new Date(sdf_judge.parse(date).getTime());
	}
	
	public static Date getDate(long date) {
		return new Date(date);
	}
	
	public static String parseTimestamp(long time) throws ParseException {
		return sdf_store.format(new Date(time)).toString();
	}
	
	public static boolean checkDateValid(String date) {
		Pattern pattern = Pattern.compile("[0-9]{8}");
		Matcher isValid = pattern.matcher(date);
		if (!isValid.matches())
			return false;
		pattern = Pattern.compile("((?!0000)[0-9]{4}"
				+ "((0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-8])"
				+ "|(0[13-9]|1[0-2])(29|30)"
				+ "|(0[13578]|1[02])31)"
				+ "|([0-9]{2}(0|[48]|[2468][048]|[13579][26])"
				+ "|(0[48]|[2468][048]|[13579][26])00)0229)");
		isValid = pattern.matcher(date);
		if (!isValid.matches())
			return false;
		return true;
	}
	
	public static String format(Date date) {
		return date == null ? null : sdf_timestamp.format(date);
	}
}
