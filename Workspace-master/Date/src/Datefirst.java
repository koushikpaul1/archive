import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.*;


public class Datefirst {

	
	public static void main (String []a){
		Date d=new Date();
		System.out.println("date is "+d+"\nTime is "+d.getTime());
		d.setTime(d.getTime()+60*1000*60);
		System.out.println("after addition\ndate is "+d);
	
		//Calendar
		System.out.println("\n\n**Calendar**");
		Calendar cal = Calendar.getInstance();// Calendar is an abstract class. can't make an object with new.
		cal.setTime(d);
		System.out.println(cal + "\n"+cal.getTime());
		
		
		
		//DateFormat
		System.out.println("\n\n**DateFormat**");
		//This is also an abstract class. can't make an object with new.
		Date d1 = new Date(1000000000000L);
		DateFormat[] dfa = new DateFormat[6];// This is an array not actual object, so new is allowed.
		//dfa[0] = new DateFormat(); Abstract
		dfa[0] = DateFormat.getInstance();
		dfa[1] = DateFormat.getDateInstance();
		dfa[2] = DateFormat.getDateInstance(DateFormat.SHORT);
		dfa[3] = DateFormat.getDateInstance(DateFormat.MEDIUM);
		dfa[4] = DateFormat.getDateInstance(DateFormat.LONG);
		dfa[5] = DateFormat.getDateInstance(DateFormat.FULL);
		for(DateFormat df : dfa)
		System.out.println(df.format(d1));
		
		
		//Local
		System.out.println("\n\n**Locale**");
		Calendar c = Calendar.getInstance();
		c.set(2010, 11, 14); // December 14, 2010
		// (month is 0-based
		Date d2 = c.getTime();
		//Locale(String language)
		//Locale(String language, String country)
		Locale locIT = new Locale("it", "IT"); // Italy
		Locale locPT = new Locale("pt"); // Portugal
		Locale locBR = new Locale("pt", "BR"); // Brazil
		Locale locIN = new Locale("hi", "IN"); // India
		Locale locJA = new Locale("ja"); // Japan
		DateFormat dfUS = DateFormat.getInstance();
		System.out.println("US " + dfUS.format(d2));
		DateFormat dfUSfull = DateFormat.getDateInstance(DateFormat.FULL);
		System.out.println("US full " + dfUSfull.format(d2));
		DateFormat dfIT = DateFormat.getDateInstance(DateFormat.FULL, locIT);
		System.out.println("Italy " + dfIT.format(d2));
		DateFormat dfPT = DateFormat.getDateInstance(DateFormat.FULL, locPT);
		System.out.println("Portugal " + dfPT.format(d2));
		DateFormat dfBR = DateFormat.getDateInstance(DateFormat.FULL, locBR);
		System.out.println("Brazil " + dfBR.format(d2));
		DateFormat dfIN = DateFormat.getDateInstance(DateFormat.FULL, locIN);
		System.out.println("India " + dfIN.format(d2));
		DateFormat dfJA = DateFormat.getDateInstance(DateFormat.FULL, locJA);
		System.out.println("Japan " + dfJA.format(d2));
		
		//Locale Display Country and language
		System.out.println("\n\n**Locale Display Country and Language**");
		Calendar c1 = Calendar.getInstance();
		c.set(2010, 11, 14);
		Date d3 = c.getTime();
		Locale locBR1 = new Locale("pt", "BR"); // Brazil
		Locale locDK = new Locale("da", "DK"); // Denmark
		Locale locIT1 = new Locale("it", "IT"); // Italy
		Locale locIN1 = new Locale("ben","IN"); // Italy
		System.out.println("def " + locBR1.getDisplayCountry());
		System.out.println("loc " + locBR1.getDisplayCountry(locBR1));
		System.out.println("def " + locDK.getDisplayLanguage());
		System.out.println("loc " + locDK.getDisplayLanguage(locDK));
		System.out.println("D>I " + locDK.getDisplayLanguage(locIT1));
		System.out.println("Ind " + locIN1.getDisplayCountry());
		System.out.println("Ind " + locIN1.getDisplayLanguage());
		System.out.println("Ind " + locIN1.getDisplayLanguage(locIN1));
		
		
		//NumberFormat
		System.out.println("\n\n**NumberFormat**");
		float f1 = 123.4567f;
		Locale locFR = new Locale("fr"); // France
		NumberFormat[] nfa = new NumberFormat[4];
		nfa[0] = NumberFormat.getInstance();
		nfa[1] = NumberFormat.getInstance(locFR);
		nfa[2] = NumberFormat.getCurrencyInstance();
		nfa[3] = NumberFormat.getCurrencyInstance(locFR);
		for(NumberFormat nf : nfa)
		System.out.println(nf.format(f1));
	}
}
