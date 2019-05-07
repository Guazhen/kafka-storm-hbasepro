package com.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateFmt {

	public static final String date_long = "yyyy-MM-dd HH:mm:ss" ;
	public static final String date_short = "yyyy-MM-dd" ;
	public static final String date_minute = "yyyyMMddHHmm" ;
	
	
	public static SimpleDateFormat sdf = new SimpleDateFormat(date_short);
	
	public static String getCountDate(String date,String patton)
	{
		SimpleDateFormat sdf = new SimpleDateFormat(patton);
		Calendar cal = Calendar.getInstance(); 
		if (date != null) {
			try {
				cal.setTime(sdf.parse(date)) ;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return sdf.format(cal.getTime());
	}
	
	public static String getCountDate(String date,String patton,int step)
	{
		SimpleDateFormat sdf = new SimpleDateFormat(patton);
		Calendar cal = Calendar.getInstance(); 
		if (date != null) {
			try {
				cal.setTime(sdf.parse(date)) ;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		cal.add(Calendar.DAY_OF_MONTH, step) ;
		return sdf.format(cal.getTime());
	}
	
	public static Date parseDate(String dateStr) throws Exception
	{
		return sdf.parse(dateStr);
	}


	//获取当前时间的X坐标
	public static String[] getXValueStr() {
		Calendar c = Calendar.getInstance();
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		//总秒数
		int curSecNum = hour * 3600 + minute * 60 + sec;

		Double xValue = (double) curSecNum / 3600;
		// 时间，横坐标
		String[] end = { hour + ":" + minute, xValue.toString() };
		return end;
	}

	public static void main(String[] args) throws Exception{

//		System.out.println(DateFmt.getCountDate("2014-03-01 12:13:14", DateFmt.date_short));
		System.out.println(parseDate("2014-05-02").after(parseDate("2014-05-01")));
	}

}
