import java.util.*;
class ScanIn {
public static void main(String[] args) {
System.out.print("input: ");
System.out.flush();
try {
Scanner s = new Scanner(System.in);//Will ask for input after running the program
String token;
do {
token = s.findInLine("\\d\\d");//reads the token if found , and stops where found
System.out.println("found " + token);
} while (token != null);
} catch (Exception e) { System.out.println("scan exc"+e.getStackTrace()); }
}
}