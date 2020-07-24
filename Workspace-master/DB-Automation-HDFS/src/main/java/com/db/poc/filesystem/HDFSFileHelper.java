package com.db.poc.filesystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class HDFSFileHelper {

	private String MACHINE_IP;
	private int MACHINE_PORT;
	private String LOGIN_USER;
	private String PASSWORD;
	private String COLPOS;

	public HDFSFileHelper(String MACHINE_IP, int MACHINE_PORT, String LOGIN_USER, String PASSWORD, String colPos) {

		this.MACHINE_IP = MACHINE_IP;
		this.MACHINE_PORT = MACHINE_PORT;
		this.LOGIN_USER = LOGIN_USER;
		this.PASSWORD = PASSWORD;
		this.COLPOS = colPos;
	}

	/**
	 * 
	 * @param sourceSystemPathToScanMapping
	 * @return
	 * @throws JSchException
	 * @throws IOException
	 */
	public List<String> getSourceFileList(Map<String, String> sourceSystemPathToScanMapping)
			throws JSchException, IOException {
		String line = null;
		List<String> inputFiles = new ArrayList<String>();
		for (Entry<String, String> sourceSysToScanPath : sourceSystemPathToScanMapping.entrySet()) {

			Session session = createSessionToConnectMachine();
			// String getFileName = "hadoop fs -ls "
			String getFileName = "hdfs dfs -ls " + sourceSysToScanPath.getValue() + "/"
			// + sourceSysToScanPath.getKey() + "*" + " | cut -d ' ' -f15";
					+ sourceSysToScanPath.getKey() + "*" + " | cut -d ' ' -" + COLPOS;

			System.out.println(getFileName);

			ChannelExec channel = (ChannelExec) session.openChannel("exec");
			((ChannelExec) channel).setCommand(getFileName);

			// channel.connect();
			channel.connect(100 * 100);

			InputStream in = channel.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			// add that file name
			while ((line = reader.readLine()) != null) {
				System.out.println("hdfs path with file name ::: " + line);
				File inputFile = new File(line);
				System.out.println("file name : " + inputFile.getName());
				inputFiles.add(inputFile.getName());
			}
			channel.disconnect();

		}
		// inputFiles from hadoop
		// :[/user/devbox/poc_input_directory/oracle-sa_trading_100.json]
		// [oracle-sa_trading_100.json]
		return inputFiles;
	}

	/**
	 * 
	 * @return
	 * @throws JSchException
	 */
	private Session createSessionToConnectMachine() throws JSchException {

		java.util.Properties config = new java.util.Properties();
		config.put("StrictHostKeyChecking", "no");

		JSch jsch = new JSch();

		Session session = jsch.getSession(LOGIN_USER, MACHINE_IP, MACHINE_PORT);
		session.setConfig(config);
		session.setPassword(PASSWORD);
		session.connect();

		return session;
	}

	/***
	 * 
	 * @param sourceFileLocation
	 * @return
	 * @throws JSchException
	 * @throws IOException
	 */
	public String getOneLine(String sourceFileLocation) throws JSchException, IOException {
		String oneLineJson = null;
		String line = null;

		Session session = createSessionToConnectMachine();
		String readOneRow = "hadoop fs -cat " + sourceFileLocation + " | head -1";
		System.out.println(readOneRow);

		ChannelExec channel = (ChannelExec) session.openChannel("exec");
		((ChannelExec) channel).setCommand(readOneRow);

		channel.connect(100 * 100);

		InputStream in = channel.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		while ((line = reader.readLine()) != null) {
			oneLineJson = line;
		}
		return oneLineJson;
	}

	/**
	 * 
	 * @param targetDiretory
	 * @param sourceFileLocation
	 * @throws JSchException
	 * @throws IOException
	 */
	public void createCopyAndDeleteHdfs(String targetDiretory, String sourceFileLocation)
			throws JSchException, IOException {

		Session session = createSessionToConnectMachine();

		String moveFile = "hadoop fs -mv " + sourceFileLocation + " " + targetDiretory;

		System.out.println(moveFile);

		ChannelExec channel = (ChannelExec) session.openChannel("exec");
		((ChannelExec) channel).setCommand(moveFile);

		channel.connect(100 * 100);

	}

	/**
	 * 
	 * @param targetDiretory
	 * @throws JSchException
	 * @throws IOException
	 */
	public void createTargetDiretoryHdfs(String targetDiretory) throws JSchException, IOException {

		Session session = createSessionToConnectMachine();
		String createDiractory = "hadoop fs -mkdir " + targetDiretory;

		System.out.println(createDiractory);

		ChannelExec channel = (ChannelExec) session.openChannel("exec");
		((ChannelExec) channel).setCommand(createDiractory);

		channel.connect(100 * 100);

	}

}