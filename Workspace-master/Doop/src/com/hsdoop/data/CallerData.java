package com.hsdoop.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class CallerData {

	static int[] ar = null;
	static int[] arr = null;

	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub
		CallerData CD = new CallerData();
		CD.fromNumber(100);
		CD.numberRecord(2622,ar);
		CD.inOutTime();
		//CD.fromNumberRecord(500, ar);
		//CD.toNumberRecord(500);
		// for(int i:arr)
		// System.out.println(i);
	}

	void fromNumber(int i) {
		ar = new int[i];
		for (int j = 0; j < i; j++) {
			ar[j] = 90000000 + (int) (Math.random() * (900000000));
		}
	}
	
	void numberRecord(int i, int[] k) throws IOException {
		arr = new int[i];int num;
		File List = new File("Callerslist.txt");
		List.createNewFile();
		PrintWriter pw = new PrintWriter(List);
		for (int j = 0; j < i; j++) {
			num=90000000 + (int) (Math.random() * (900000000));
			pw.println("9" + num+"	"+"9" + k[(int) (Math.random() * (k.length))]);
		}
		pw.close();
	}
	
	void inOutTime() throws IOException, ParseException{
		
		List<String> records = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader("time.txt"));
		String line; Calendar c = Calendar.getInstance();
		File file = new File("duration.txt");
		file.createNewFile();
		PrintWriter pw = new PrintWriter(file);
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf1=new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		while ((line = reader.readLine()) != null)
			
			
			
		{
			records.add(line);
			
			String dateInString = line;	
			Date date = formatter.parse(dateInString);			
			c.setTime(date);
		    c.add(Calendar.MILLISECOND, (int) (Math.random() * (30000000)));					
			pw.println(sdf1.format(date)+","+sdf.format(c.getTime()));
		}
		pw.close();
		reader.close();
		
		
		
		
	}
	
	
	/*void fromNumberRecord(int i, int[] k) throws IOException {
		arr = new int[i];
		String fnum = "9";
		File List = new File("Callerlist.txt");
		List.createNewFile();
		PrintWriter pw = new PrintWriter(List);
		for (int j = 0; j < i; j++) {
			// arr[j]= k[(int)(Math.random() * (k.length))];
			pw.println(fnum + k[(int) (Math.random() * (k.length))]);
		}
		pw.close();
	}
	
	void toNumberRecord(int i) throws IOException {		
		File List = new File("Receiverlist.txt");int num;
		List.createNewFile();
		PrintWriter pw = new PrintWriter(List);
		for (int j = 0; j < i; j++) {
			num=90000000 + (int) (Math.random() * (900000000));
			pw.println("9" + num);
		}
		pw.close();
	}*/
}
