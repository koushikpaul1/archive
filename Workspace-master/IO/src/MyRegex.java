import java.util.regex.*;


public class MyRegex {
	 
	public static void main(String [] args) {
		Pattern p = Pattern.compile("ab"); // the expression
		Matcher m = p.matcher("abaaaba"); // the source
		while(m.find()) {
		System.out.print(m.start() + ",");}//0,4,
		
		
		/*
		"\d"=digit			
		"\s"=space
		"\w"=character
		*/
		
		
		System.out.println("\n\n*******");
		p=Pattern.compile("\\d");
		m=p.matcher("8khvkcjh8hcvkchv0-bbfhb2bdkjv33");
		
		while(m.find()){
			System.out.print(m.start()+",");//0,8,16,23,29,30,
		}
		
		
		/*
		[abc]=a or b or c 
		like		\d\d[abc]\d=12b3
		*/
		
		System.out.println("\n\n*******");
		p=Pattern.compile("[abc]");
		m=p.matcher("akjiabcijiabijiabfkkhacdghgacehkjkecd");
		
		while(m.find()){
			System.out.print(m.start()+",");//0,4,5,6,10,11,15,16,21,22,27,28,35,
		}
		
		System.out.println("\n\n*******");
		p=Pattern.compile("[a-f]");//anything between a-f
		m=p.matcher("akjiabcijiabijiabfkkhacdghgacehkjkecd");
		
		while(m.find()){
			System.out.print(m.start()+",");//0,4,5,6,10,11,15,16,17,21,22,23,27,28,29,34,35,36,
		}
		
		System.out.println("\n\n*******");
		p=Pattern.compile("[aA]");//a or A ,[a-cA-C] searches for a or b or c or A or..
		m=p.matcher("akjiabcAijiabijAiabfkkhacdghgacehkjkecd");
		
		while(m.find()){
			System.out.print(m.start()+",");//0,4,7,11,15,17,23,29,
		}
		
			
		
		System.out.println("\n\n*******");
		p=Pattern.compile("0[xX][0-9a-fA-F]");//Find a set of characters in which the first character is a "0", the second character is either an "x" or an "X", and the third character is either a digit from "0" to "9", a letter from "a" to "f" or an uppercase letter from "A" to "F" "
		m=p.matcher("12 0x 0x12 0Xf 0xg");
		
		while(m.find()){
			System.out.print(m.start()+",");//6,11,
		}
		
		System.out.println("\n\n*******");
		p=Pattern.compile("\\d+");//  \d+=the whole number
		m=p.matcher("8khvkcjh509hcvk");
		
		while(m.find()){
			System.out.println(m.start()+","+m.group());//0,8
														//8,509
		}
		
		System.out.println("\n\n*******");
		p=Pattern.compile("proj1([^,])*");//  [^,]  =anything other than ","// *=zero or any time, ?= 0 or 1 time
		m=p.matcher("proj3.txt,proj1sched.pdf,proj1,proj2,proj1.java");
		
		while(m.find()){
			System.out.println(m.start()+","+m.group());//10,proj1sched.pdf
														//25,proj1
														//37,proj1.java
		}
		
		
		System.out.println("\n\n*******");
		p=Pattern.compile("a.c");//"."=anything
		m=p.matcher("abc axvc a catgac");
		
		while(m.find()){
			System.out.println(m.start()+","+m.group());//0,abc
														//9,a c
		}
		
		
		/*? is greedy, ?? is reluctant, for zero or once
		* is greedy, *? is reluctant, for zero or more
		+ is greedy, +? is reluctant, for one or more*/
		
		System.out.println("\n\n*******");
		p=Pattern.compile(".*xx");//
		m=p.matcher("yyxxxyxx");
		
		while(m.find()){
			System.out.println(m.start()+","+m.group());}//0,0,yyxxxyxx
														
		p=Pattern.compile(".*?xx");//
		m=p.matcher("yyxxxyxx");
			
		while(m.find()){
			System.out.println(m.start()+","+m.group());//0,yyxxx
														//4,xyxx
		}
		
		
		
		
		
		
		
}}
