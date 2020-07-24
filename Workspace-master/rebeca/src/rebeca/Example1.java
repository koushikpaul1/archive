package rebeca;

public class Example1 {
	static int [] data={2,2,1,0,1};
	
	public static void main (String [] a){
		Example1 obj=new Example1();
		obj.solution(data,5);
		
		
	}
	
	int solution(int A[], int n) {
		int [] temp=new int [n];
		int counter=0;
		boolean flag=true;
		
		for (int i=0;i<n;i++){
			for (int j=0;j<=counter;j++){
				if( A[i]==temp[j] ){
					flag=false;break;}
			}if (flag==true){
				temp[counter]=A[i];
				counter++;
			}else{flag=true;}
		}for (int i=0;i<n;i++)System.out.println(temp[i]);
		System.out.println("length = "+counter);
	    return n;
	}
}
