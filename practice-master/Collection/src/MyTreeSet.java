import java.util.*;
public class MyTreeSet //implements Comparable
{
	
	public static void main(String [] a){
		TreeSet<MyTreeSet> at= new TreeSet<MyTreeSet>();
		at.add(new MyTreeSet());
		at.add(new MyTreeSet());
		at.add(new MyTreeSet());
		at.add(new MyTreeSet());
		
		System.out.println("Size= "+at.size());
		System.out.println(at.first());
	}

	/*public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}*/



}

//Error at Runtime because MyTreeSet doesnt implements Comparable
