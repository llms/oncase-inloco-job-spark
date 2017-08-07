package validaCoordenada;

import java.math.BigDecimal;

public class ValidarCoordenada {
	public static float distFrom(float lat1, float lng1, float lat2, float lng2) {
        float earthRadius = 6371000; //meters
     
        float dLat = Math.toRadians(lat2-lat1);
        float dLng = new BigDecimal(Math.toRadians(lng2.floatValue()-lng1.floatValue()));
        float a = new BigDecimal( Math.sin(dLat.floatValue()/2) * Math.sin(dLat.floatValue()/2) +
                   Math.cos(Math.toRadians(lat1.floatValue())) * Math.cos(Math.toRadians(lat2.floatValue())) *
                   Math.sin(dLng.floatValue()/2) * Math.sin(dLng.floatValue()/2));
        float c =  new BigDecimal(2 * Math.atan2(Math.sqrt(a.floatValue()), Math.sqrt(1-a.floatValue())));
        float dist =  new BigDecimal(earthRadius.floatValue() * c.floatValue());

        return dist;
      }
    
	
	
	public static void main(String[] args) {
		Double double1 = new Double(-8.124891047);
	   double1.floatValue();
		System.out.println(double1);
		/*
		BigDecimal lat1 = new BigDecimal( -8.124891047).setScale(20, BigDecimal.ROUND_HALF_UP);
		BigDecimal lng1 = new BigDecimal(-34.901991059).setScale(20, BigDecimal.ROUND_HALF_UP);
		BigDecimal lat2 = new BigDecimal(-8.125111).setScale(20, BigDecimal.ROUND_HALF_UP);
		BigDecimal lng2 = new BigDecimal(-34.9017837).setScale(20, BigDecimal.ROUND_HALF_UP);
		BigDecimal metros =  ValidarCoordenada.distFrom(lat1, lng1, lat2, lng2);
		System.out.println(metros);*/
	}
}
