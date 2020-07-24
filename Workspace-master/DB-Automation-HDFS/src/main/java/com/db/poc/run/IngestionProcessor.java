package com.db.poc.run;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.db.poc.exception.JSONSchemaCompareException;
import com.db.poc.filesystem.FileSystemHelper;
import com.db.poc.filesystem.HDFSFileHelper;
import com.db.poc.hive.HiveServer2API;
import com.db.poc.json.HiveJSONSerdeExternalTableGenerator;
import com.db.poc.json.JsonToJsonSchemaCompare;
import com.db.poc.sql.DBAPIService;

public class IngestionProcessor {

	static HiveServer2API hiveconn;
	static HDFSFileHelper hdfsHelper;
	static DBAPIService sql;

	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			throw new Exception("Please provide " + 4 + " arguments in the below order :  "
					+ "  \n   1. Scan duration           <number in miliseconds>        "
					+ "  \n   2. Hive2 details in format <ip:port:database:user:pass>   "
					+ "  \n   3. Metadata database (db-->MySQL/Postgre) details in format <db:ip:port:database:user:pass>   "
					+ "  \n   4. HDFS details in format  <ip:port:user:pass:colpos>             ");
		}

		// 1. Scan duration
		long scanDuration = Long.parseLong(args[0]);
		System.out.println(" scanDuration " + scanDuration);

		int hive2Len = args[1].split(":").length;
		if (hive2Len != 5) {
			throw new Exception(" supply 5 property in second argument for Hive2");
		}
		String[] hive2Arg = args[1].split(":");

		// 2. Hive2 details in format
		String hive2IP = hive2Arg[0];
		int hive2Port = Integer.parseInt(hive2Arg[1]);
		String hive2DB = hive2Arg[2];
		String hive2UserId = hive2Arg[3];
		String hive2Pass = hive2Arg[4];

		System.out.println(" hive2IP   " + hive2IP);
		System.out.println(" hive2Port   " + hive2Port);
		System.out.println(" hive2DB   " + hive2DB);
		System.out.println(" hive2UserId   " + hive2UserId);
		System.out.println(" hive2Pass   " + hive2Pass);

		hiveconn = new HiveServer2API(hive2IP, hive2Port, hive2DB, hive2UserId, hive2Pass);

		int mySqlLen = args[2].split(":").length;
		if (mySqlLen != 6) {
			throw new Exception(" supply 6 property in third argument for Metadata database");
		}

		// 3. Metadata database details
		String dbOption = args[2].split(":")[0];
		String sqlIP = args[2].split(":")[1];
		int sqlPort = Integer.parseInt(args[2].split(":")[2]);
		String sqlDBName = args[2].split(":")[3];
		String sqlUserId = args[2].split(":")[4];
		String sqlPass = args[2].split(":")[5];

		System.out.println(" dbOption selected   " + dbOption);
		System.out.println(" SqlIP   " + sqlIP);
		System.out.println(" SqlPort   " + sqlPort);
		System.out.println(" SqlDB   " + sqlDBName);
		System.out.println(" SqUserId   " + sqlUserId);
		System.out.println(" SqlPass   " + sqlPass);

		sql = new DBAPIService(dbOption,sqlIP, sqlPort, sqlDBName, sqlUserId, sqlPass);

		int hdfsLen = args[3].split(":").length;
		if (hdfsLen != 5) {
			throw new Exception(" supply 5 property in fourth argument for HDFS ");
		}
		// 4. HDFS details in format
		String hdfsIP = args[3].split(":")[0];
		int hdfsPort = Integer.parseInt(args[3].split(":")[1]);
		String hdfsUser = args[3].split(":")[2];
		String hdfsPass = args[3].split(":")[3];
		String colPos = args[3].split(":")[4];
		System.out.println(" hdfsIP   " + hdfsIP);
		System.out.println(" hdfsPort   " + hdfsPort);
		System.out.println(" hdfsUser   " + hdfsUser);
		System.out.println(" hdfsPass   " + hdfsPass);
		System.out.println(" column position   " + colPos);
		hdfsHelper = new HDFSFileHelper(hdfsIP, hdfsPort, hdfsUser, hdfsPass,colPos);

		while (true) {
			// 10 seconds
			// Thread.sleep(25000); --> moved this to at end

			List<Map<String, String>> allSourceList = sql.getSourceCatalogDetails();
			System.out.println(" allSourceList " + allSourceList);
			FileSystemHelper fileSysHelp = new FileSystemHelper();

			// Use data base to get
			Map<String, String> sourceSystemToScanPath = getSourceSystemAndScanedLocation(allSourceList);
			System.out.println(" sourceSystemToScanPath " + sourceSystemToScanPath);
			// scan source location for valid on boarded system, keep window
			// currently, change to hdfs later. if files available. get the file
			Map<String, String> sourceSystemTovalidFiles = getValidFiles(sourceSystemToScanPath, fileSysHelp);

			// get configuration sets in files.
			Map<String, List<String>> sourceDetailsConfig = getDetailsAboutSource(allSourceList);
			System.out.println(" sourceDetailsConfig ------------->" + sourceDetailsConfig);
			// System.out.println(" validFiles ------------->" + validFiles);
			for (Entry<String, String> validFile : sourceSystemTovalidFiles.entrySet()) {
				// oracle-sa_customer
				String sourceNameKey = validFile.getKey();
				// D:\sourceLocation\oracle\oracle-sa_emp_ddfdfgdgd.json
				String sourceFileLocation = validFile.getValue();

				// Commented line for window OS
				// String oneLineJson =
				// fileSysHelp.readOneLine(sourceFileLocation);
				String oneLineJson = hdfsHelper.getOneLine(sourceFileLocation);

				System.out.println(" oneLineJson ------------->" + oneLineJson);
				System.out.println(" sourceNameKey ------------->" + sourceNameKey);
				// query the data base and get latest version and schema for
				// given source system
				List<String> schemaVersionLatest = sql.getlatestJSONSchmaForGivenSource(sourceNameKey);

				// first time ingestion
				if (schemaVersionLatest.size() == 0) {
					// Insert in mysql db, with schema json, version 1, auto
					// create external table definition
					createExternalTableAndSaveRecord(sourceNameKey, sourceDetailsConfig, 1, oneLineJson, sql);
					// String targetDiretory =
					// sourceDetailsConfig.get(sourceNameKey).get(1) +
					// File.separator + 1;
					String targetDiretory = sourceDetailsConfig.get(sourceNameKey).get(1) + "/" + 1;

					// Create targetDiretory
					hdfsHelper.createTargetDiretoryHdfs(targetDiretory);

					hdfsHelper.createCopyAndDeleteHdfs(targetDiretory, sourceFileLocation);

				} else {
					// latest schema and version found for given key
					JsonToJsonSchemaCompare json2json = new JsonToJsonSchemaCompare();
					int oldVersion = Integer.parseInt(schemaVersionLatest.get(1));
					String oldDirectory = sourceDetailsConfig.get(sourceNameKey).get(1) + "/" + oldVersion;
					try {
						json2json.compareJSON(oneLineJson, schemaVersionLatest.get(0));
						hdfsHelper.createCopyAndDeleteHdfs(oldDirectory, sourceFileLocation);
					} catch (JSONSchemaCompareException jsonExe) {
						int newVersion = oldVersion + 1;
						String newDirectory = sourceDetailsConfig.get(sourceNameKey).get(1) + "/" + newVersion;
						createExternalTableAndSaveRecord(sourceNameKey, sourceDetailsConfig, newVersion, oneLineJson,
								sql);
						// add change in latest version
						sql.updateEndDateSchemaCatalog(sourceNameKey, currentDateInFormat(),
								jsonExe.getMessage().replaceAll("com.poc.exception.JSONSchemaCompareException:", ""),
								newVersion);
						sql.updateEndDateSchemaCatalog(sourceNameKey, currentDateInFormat(), "", oldVersion);

						// when there is change
						hdfsHelper.createCopyAndDeleteHdfs(newDirectory, sourceFileLocation);
					} catch (Exception exe) {
						System.out.println(" Exception " + exe);

					}
				}

			}
			Thread.sleep(scanDuration);
		}
	}

	private static void createExternalTableAndSaveRecord(String sourceNameKey, Map<String, List<String>> sourceDetails,
			int version, String validSchema, DBAPIService mysql) {
		String newTableName = sourceDetails.get(sourceNameKey).get(0) + "_" + version;
		// String newHDFSLocation = sourceDetails.get(sourceNameKey).get(1) +
		// File.separator + version;
		String newHDFSLocation = sourceDetails.get(sourceNameKey).get(1) + "/" + version;
		System.out.println("newHDFSLocation :" + newHDFSLocation);
		HiveJSONSerdeExternalTableGenerator hiveTable = new HiveJSONSerdeExternalTableGenerator();
		String def = hiveTable.generateDefinition(validSchema, newTableName, newHDFSLocation);

		mysql.insertFirstRecordSchemaCatalog(sourceNameKey, validSchema, new Integer(version), newTableName,
				def.replaceAll("'", "''"));

		System.out.println("Hive table def :" + def);
		boolean result = hiveconn.executeSQL(def);
		System.out.println("after creating external table :" + result);

	}

	private static String currentDateInFormat() {
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // dd/MM/yyyy
		Date now = new Date();
		String strDate = sdfDate.format(now);
		return strDate;
	}

	private static Map<String, List<String>> getDetailsAboutSource(List<Map<String, String>> allSourceLists) {
		Map<String, List<String>> details = new HashMap<String, List<String>>();
		for (Map<String, String> allSourceList : allSourceLists) {
			List<String> fields = new ArrayList<String>();
			fields.add(allSourceList.get(DBAutomationConstant.HIVE_TABLE_NAME_PREFIX));
			fields.add(allSourceList.get(DBAutomationConstant.TARGET_HDFS_LOCATION));
			details.put(allSourceList.get(DBAutomationConstant.KEY_SOURCECATALOG), fields);
		}
		return details;
	}

	private static Map<String, String> getValidFiles(Map<String, String> sourceSystemPathToScanMapping,
			FileSystemHelper fileSysHelp) throws Exception {
		Map<String, String> validResults = new HashMap<String, String>();
		// oracle-sa-tradin --> /user/devbox/poc_input_directory
		for (Entry<String, String> result : sourceSystemPathToScanMapping.entrySet()) {
			/**
			 * List out all mapping files present at HDFS and return them into
			 * List<String>
			 */
			List<String> inputFiles = hdfsHelper.getSourceFileList(sourceSystemPathToScanMapping);
			System.out.println("sourceSystemPathToScanMapping  :" + result.getValue());
			System.out.println("inputFiles from hadoop  :" + inputFiles);
			for (String file : inputFiles) {
				if (file.contains(result.getKey())) {
					validResults.put(result.getKey(), result.getValue() + "/" + file);
				}
			}
		}
		System.out.println("validResults  :" + validResults);
		return validResults;
	}

	private static Map<String, String> getSourceSystemAndScanedLocation(List<Map<String, String>> allSourceList) {
		Map<String, String> toReturn = new HashMap<String, String>();
		for (Map<String, String> results : allSourceList) {
			for (Entry<String, String> res : results.entrySet()) {
				if (res.getKey().equals(DBAutomationConstant.KEY_SOURCECATALOG)) {
					toReturn.put(res.getValue(), results.get(DBAutomationConstant.INPUT_FILE_LOCATION));
				}
			}
		}
		return toReturn;
	}

}
