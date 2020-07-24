package com.db.poc.filesystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 
 * @author Kartik_Bhatnagar
 *
 */

public class FileSystemHelper {

	private static final String osCommand = "cmd /c dir /b";

	/**
	 * Writes given content to the given file. The file must be file with full
	 * path.
	 *
	 * @param fileName
	 * @param content
	 * @throws Exception
	 */
	public void writeFile(final String fileName, final String content)
			throws Exception {

		final FileOutputStream fos = new FileOutputStream(fileName, false);
		final OutputStreamWriter writer = new OutputStreamWriter(fos, "UTF-8");
		writer.write(content);
		writer.close();
		fos.close();

	}

	/**
	 * Reads the file from the given full path.
	 *
	 * @param filePath
	 * @return
	 * @throws Exception
	 */
	public String readFile(final String filePath) throws Exception {
		final StringBuffer buffer = new StringBuffer();
		final FileInputStream fis = new FileInputStream(filePath);
		final InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
		final Reader reader = new BufferedReader(isr);
		int character;
		while ((character = reader.read()) > -1) {
			buffer.append((char) character);
		}
		reader.close();
		isr.close();
		fis.close();
		return buffer.toString();
	}

	/**
	 * Lists the files from the given directory location.
	 *
	 * @param dir
	 * @return
	 * @throws Exception
	 */
	public List<String> listFiles(final File dir) throws Exception {
		final List<String> files = new ArrayList<String>();

		final Process process = Runtime.getRuntime().exec(
				osCommand + " " + dir.getAbsolutePath());
		final InputStream inputStream = process.getInputStream();

		int character;
		final StringBuilder buffer = new StringBuilder();
		while (((character = inputStream.read()) != -1)) {
			if (character == '\n') {
				files.add(buffer.toString().replaceAll("(\\r|\\n)", ""));
				buffer.setLength(0);
			} else {
				buffer.append((char) character);
			}
		}
		if (buffer.length() > 0) {
			files.add(buffer.toString().replaceAll("(\\r|\\n)", ""));
		}
		return files;
	}

	/**
	 * Moves the given file from source to target directory location.
	 *
	 * @param sourceDir
	 * @param targetDir
	 * @param fileName
	 * @throws Exception
	 */
	public void move(final String sourceDir, final String targetDir,
			final String fileName) throws Exception {
		move(targetDir, sourceDir + File.separator + fileName);
	}

	/**
	 * Moves the files to target location. Here the file is with full path.
	 *
	 * @param targetDir
	 * @param fileName
	 * @throws Exception
	 */
	public void move(final String targetDir, final String fileName)
			throws Exception {

		final File sourceFile = new File(fileName);

		System.out.println("  move sourceFile.getName() "
				+ sourceFile.getName());
		try {
			boolean success = sourceFile.renameTo(new File(targetDir + "\\"
					+ sourceFile.getName()));

			if (!success) {
				success = sourceFile.renameTo(new File(targetDir + "\\"
						+ sourceFile.getName()));
			}

			if (!success) {
				throw new Exception("can't move file " + targetDir + "\\"
						+ sourceFile.getName());
			}
		} catch (Exception exe) {
			System.out.println(" move exe ----------> " + exe);
		}
		/*
		 * if (!success) { throw new Exception("file move failed"); }
		 */
	}

	public void delete(final String path) {
		final File sourceFile = new File(path);
		sourceFile.delete();
	}

	/**
	 * Creates local store in EC2 instance.
	 *
	 * @return
	 */
	public String createLocalStore(final String ec2Path) {
		final File dir = new File(preparePath(ec2Path, UUID.randomUUID()
				.toString()));
		dir.mkdir();
		return dir.getAbsolutePath();
	}

	public String createDirectory(final String path) {
		final File dir = new File(path);
		dir.mkdir();
		return dir.getAbsolutePath();
	}

	/**
	 * Prepares the file path
	 *
	 * @param pathParams
	 * @return
	 */
	public String preparePath(final String... pathParams) {
		final StringBuilder path = new StringBuilder();
		boolean first = true;
		for (final String pathParam : pathParams) {
			if (first) {
				first = false;
			} else {
				path.append(File.separator);
			}

			path.append(pathParam);
		}

		return path.toString();
	}

	/**
	 * Extracts the extension from a filename
	 *
	 * @param pathName
	 * @return
	 */
	public String extractExtension(final String pathName) {

		String extension = null;
		final String[] tokens = pathName.split("\\.");
		if (tokens.length > 0) {
			extension = tokens[tokens.length - 1];
		}

		return extension;
	}

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

}
