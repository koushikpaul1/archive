import java.util.TreeSet;
class Super1{
	static void main1() {
		System.out.println(" Super");
	}
}

public class TestClassTree extends Super1 {
static void main1() throws ArithmeticException{
		System.out.println("child");
	}
	
	public static void main (String [] a){
		
		/*TreeSet  set1= new TreeSet();
		set1.add(new TestClassTree());
		set1.add(new TestClassTree());
		set1.add(new TestClassTree());
		System.out.println(set1.first());*/
		TestClassTree test=new TestClassTree();
		test.main1();
		
		
	}

}
