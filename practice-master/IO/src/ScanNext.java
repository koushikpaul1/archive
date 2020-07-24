import java.util.Scanner;
class ScanNext {
public static void main(String [] args) {
boolean b2, b;
int i;
String s, hits = " ";
Scanner s1 = new Scanner("Hi true it is 107");
Scanner s2 = new Scanner("Hi true it is 107");
s1.useDelimiter("i");
while(b = s1.hasNext()) {
s = s1.next(); hits += "s";
System.out.println(s);
}
while(b = s2.hasNext()) {
if (s2.hasNextInt()) {
i = s2.nextInt(); hits += "i";
} else if (s2.hasNextBoolean()) {
b2 = s2.nextBoolean(); hits += "b";
} else {
s2.next(); hits += "s2";
}
}
System.out.println("hits " + hits);
}
}