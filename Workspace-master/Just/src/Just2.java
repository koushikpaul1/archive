import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Just2 {
	
	public static  void main (String arg []) {
		
		boolean b =new justx() instanceof Justy;
		System.out.println(b);
		
		
		
	}
	
	
	/*void test (){
		Pattern p= Pattern.compile("Asutosh Behara");
		Matcher m=p.matcher("tosh");
		m.find();
		System.out.println( m.group()); 
		
		
	}*/

}

final class justx extends Justy{
	
	
}

class Justy {
	
	
	
}