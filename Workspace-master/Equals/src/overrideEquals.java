class Profile{
	private int a;
	public Profile(int a){
		this.a=a;
	}
	private Boolean equals (Profile oe){
		return oe.a==this.a;
	}
	
	
}
public class overrideEquals {
	public static void main ( String  []a){
		Profile pf1=new Profile(4);
		Profile pf2=new Profile(4);
		Object o=pf1.equals(pf2);
		System.out.println(o);
		
	}
	

}


