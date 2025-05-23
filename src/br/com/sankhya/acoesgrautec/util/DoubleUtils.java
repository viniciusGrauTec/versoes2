package br.com.sankhya.acoesgrautec.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class DoubleUtils {

	public static double roundUp(double number, int places) {
		return new BigDecimal(number)
					.setScale(places, RoundingMode.UP)
					.doubleValue();
	}


}
