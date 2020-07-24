package com.db.run;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.db.poc.filesystem.FileSystemHelper;

public class FileSystemHelperTest {

	@Test
	public void listFilesTest() throws Exception {

		String FILE_PATH = "D:\\sourceLocation\\oracle";
		FileSystemHelper fileSysHelp = new FileSystemHelper();
		final List<String> files = fileSysHelp.listFiles(new File(FILE_PATH));
		for (final String file : files) {
			System.out.println(" full path ---- " + FILE_PATH+"\\"+file);

		}
	}
	
	@Test
	public void testMoveFile() throws Exception {
		FileSystemHelper fileSysHelp = new FileSystemHelper();		
		fileSysHelp.move("D:\\targetLocation\\oracle\1", "D:\\sourceLocation\\oracle\\oracle-sa_emp.json.fix");
	}
	
	
	@Test
	public void testCreateDirectoryFull() throws Exception {	
		FileSystemHelper fileSysHelp = new FileSystemHelper();		
	
		fileSysHelp.createDirectory("D:\\targetLocation\\oracle\\55");
		fileSysHelp.move("D:\\targetLocation\\oracle\\55", "D:\\sourceLocation\\oracle\\oracle-sa_customer.json.fix_1");
		
		fileSysHelp.createDirectory("D:\\targetLocation\\oracle\\66");
		fileSysHelp.move("D:\\targetLocation\\oracle\\66", "D:\\sourceLocation\\oracle\\oracle-sa_customer.json.fix_2");
		
		
	}
}
