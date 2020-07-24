package rebeca;

public class WholeSquare {

	public static void main(String[] args) {
		
		WholeSquare ws=new WholeSquare();		
		System.out.println(ws.solution(1, 100));

	}
	
	int  solution(int a,int b){
		double sqr=0;int sqrr=0;int counter =0;
		
		for (int i=a;i<=b;i++){
			sqr=Math.sqrt(i);		
			sqrr=(int)sqr;
			if (sqrr==sqr){
				counter++;break;}
		}
		while(sqr*sqr<=b){sqr++;counter++;}
		return counter-1;
	}

}
