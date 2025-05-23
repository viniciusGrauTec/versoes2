package br.com.sankhya.acoesgrautec.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

	public static String convertTimestampToStringDate(Timestamp timestamp) {
		if (timestamp == null)
			return "";
		
		return new SimpleDateFormat("dd/MM/yyyy").format(new Date(timestamp.getTime()));
	}

	public static Date convertStringToDate(String dtReferencia) throws ParseException {
		return new SimpleDateFormat("dd/MM/yyyy").parse(dtReferencia);
	}

}
