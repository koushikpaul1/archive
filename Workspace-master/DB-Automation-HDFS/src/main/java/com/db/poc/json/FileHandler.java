package com.db.poc.json;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class FileHandler {

	public String read(String fileName) {
		String everything = null;
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			everything = sb.toString();

			return everything;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return everything;
	}
	
	
	public String readOneLine(String fileName) {
		String line = null;
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
				 line = br.readLine();	    	
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return line;
		
	}
	
	
	
	public void writeFile(String fileName, String content) {
		try {
			FileOutputStream fos = new FileOutputStream(fileName, false);
			OutputStreamWriter writer = new OutputStreamWriter(fos, "UTF-8");

			writer.write(content);
			writer.close();
			fos.close();
		} catch (IOException exe) {
		}

	}

}
