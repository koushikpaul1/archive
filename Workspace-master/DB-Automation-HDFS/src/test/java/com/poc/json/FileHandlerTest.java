package com.poc.json;

import java.io.File;

import org.junit.Test;

import com.db.poc.json.FileHandler;

public class FileHandlerTest {

	@Test
	public void testFileHandler() throws Exception {

		File inputFile = new File("src/main/resources/oracle-sa_customer.json.fix");

		FileHandler fh = new FileHandler();

		System.out.println(" one line " + fh.readOneLine(inputFile.getAbsolutePath()));

	}
	
	@Test
	public void testMoveGile() throws Exception {

		File inputFile = new File("src/main/resources/oracle-sa_trading.json.fix");

		

		//System.out.println(" one line " + fh.readOneLine(inputFile.getAbsolutePath()));

	}

}
