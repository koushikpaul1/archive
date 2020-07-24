/**
 * 
 */
package rebeca;

/**
 * @author Domino
 *
 */
public class FirstUnique {

	/**
	 * @param args
	 */
	
	static int [] data={2,2,3,1,1,5,1,9,9,5,3};
	
	public static void main(String[] args) {
		FirstUnique fq=new FirstUnique();
		System.out.println(fq.solution(data));

	}

	
	public int solution(int[] A) {
		boolean flag = true;

		for (int i = 0; i < A.length; i++) {
			for (int j = 0; j < A.length; j++)
				if (i != j) {
					if (A[i] == A[j]) {
						flag = false;
						break;
					}
				}
			if (flag == true) {
				return A[i];
			}
			flag = true;
		}
		return -1;
	}
	
	
	
}
