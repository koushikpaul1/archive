
public class Equals {
	
	public static void main (String [] aa){
		
		String a="abc";
		String b="abc";
		String c=new String("abc");
		String d=new String("abc");
		String e=a;
		
		
		System.out.println(a==b);
		System.out.println(a==c);
		System.out.println(c==d);
		System.out.println(a==e);
		
		System.out.println("\n\n");
		
		System.out.println(a.equals(b));
		System.out.println(a.equals(c));
		System.out.println(d.equals(c));
		System.out.println(a.equals(e));
		
		System.out.println("\n\n");
		
		System.out.println(new String("test").equals("test"));
		System.out.println(new String("test") == "test");
		System.out.println(new String("test") == new String("test"));
		System.out.println("test" == "test");
		
		
	}

}
