package com.edge.collection.practice;

import java.util.HashMap;
import java.util.Hashtable;

public class Test {

	public static void main(String[] args) {
	Hashtable ht=new Hashtable();
	HashMap hm= new HashMap();
	System.out.println(hm.putIfAbsent("key","value1"));;
	System.out.println(hm.putIfAbsent("key","value2"));;
	}

}