package com.vector;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class timeTest {
	static int instancia=2;
	static String HOUR_LIMIT1="00";
	static String HOUR_LIMIT2="20";
	static String EXPLOIT_DATE_PROPERTY1="2020/07/02-22:48:56";
	static String EXPLOIT_DATE_PROPERTY2="2020/07/03-01:29:42";
	static String EXPLOIT_DATE_PROPERTY3="2020/07/03-12:26:49";

	public static void main(String args[]) throws InterruptedException, ParseException {
		String EXPLOIT_DATE_PROPERTY = EXPLOIT_DATE_PROPERTY3;
		Date exploitDate = getDate(EXPLOIT_DATE_PROPERTY);
		String limit1 = HOUR_LIMIT1;
		String limit2 = HOUR_LIMIT2;
		Calendar calLimit1 = Calendar.getInstance();
		calLimit1.set(Calendar.HOUR_OF_DAY, Integer.parseInt(limit1));
		calLimit1.set(Calendar.MINUTE, 0);
		calLimit1.set(Calendar.SECOND, 0);
		calLimit1.set(Calendar.MILLISECOND, 0);

		Calendar calLimit2 = Calendar.getInstance();
		calLimit2.set(Calendar.HOUR_OF_DAY, Integer.parseInt(limit2));
		calLimit2.set(Calendar.MINUTE, 0);
		calLimit2.set(Calendar.SECOND, 0);
		calLimit2.set(Calendar.MILLISECOND, 0);

		Date now = new Date();

		Date paramDate = new SimpleDateFormat("yyyyMMdd").parse("20200703"); //EXPLOIT_DATE_PROPERTY.substring(0,10));
		SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
		System.out.println("Conversión con TimeZone: "+format.format(paramDate));

		if (now.compareTo(calLimit1.getTime()) <= 0) {
			//igual
		} else if (now.compareTo(calLimit1.getTime()) > 0 && now.compareTo(calLimit2.getTime()) < 0) {
			//igual
		} else {
			//+1 día
			Calendar cal = Calendar.getInstance();
			cal.setTime(exploitDate);
			cal.add(Calendar.DAY_OF_YEAR, 1);
			exploitDate = cal.getTime();
		}
		System.out.println("Para la fecha: " + EXPLOIT_DATE_PROPERTY);
		SimpleDateFormat formattz = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
		System.out.println("La fecha calculada es: "+formattz.format(exploitDate));
	}

	static private Date getDate(String propertyName){
		String date=(propertyName);
		Date processDate = new Date();
		if (StringUtils.isNotEmpty(date)){
			SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
			try {
				processDate=format.parse(date);
			}catch(ParseException pe){
				System.out.println(String.format("La cadena indicada %s como fecha de proceso debe cumplir el formato yyyy/MM/dd-HH:mm:ss ",date));
			}
		}
		return processDate;

	}



}
