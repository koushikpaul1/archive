package com.db.poc.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Kartik_Bhatnagar
 *
 */
public class DBAPIService extends SQLDBAPI {

	public DBAPIService(String DB_OPTION, String IP, int PORT,String DBName, String DBUSER, String DBPASS) {
		super(DB_OPTION,IP, PORT, DBName, DBUSER, DBPASS);
	}

	/**
	 * insert row into db for first schema
	 * 
	 * @param key
	 * @param jsonSchema
	 * @param version
	 * @param hiveTableName
	 * @param externalHiveTable
	 */
	
	public void insertFirstRecordSchemaCatalog(String key, String jsonSchema,
			Integer version, String hiveTableName, String externalHiveTable) {
		String sql = "insert into schemacatalog (schema_key,jsonschema,version,hivetablename,hivetabledef) VALUES ('"
				+ key
				+ "','"
				+ jsonSchema
				+ "',"
				+ "'"
				+ version
				+ "',"
				+ "'"
				+ hiveTableName + "'," + "'" + externalHiveTable + "'); ";
		System.out.println(" insertIntoTable : " + sql);
		runSqlCommand(sql);
	}

	//
	/**
	 * update end date, changed
	 * 
	 * @param key
	 * @param endDate
	 * @param changed
	 * @param version
	 */
	public void updateEndDateSchemaCatalog(String key, String endDate,
			String changed, int version) {
		// String sql = "update schemacatalog set enddate = '" + endDate + "' "
		// + ", change = '" + change
		String sql = "update schemacatalog set enddate = '" + endDate + "' "
				+ ", changed = '" + changed + "' where schema_key = '" + key
				+ "' and  version= '" + version + "' ";
		System.out.println(" updateEndDate : " + sql);
		runSqlCommand(sql);
	}

	/**
	 * get schema and version for latest key
	 * 
	 * @param key
	 * @return
	 */
	public List<String> getlatestJSONSchmaForGivenSource(String key) {
		String sql = "SELECT jsonschema,version FROM schemacatalog where schema_key='"
				+ key
				+ "' and version = (select max(version) from schemacatalog where schema_key = '"
				+ key + "')";

		System.out.println(" getlatestJSONSchmaForGivenSource sql " + sql);
		List<String> fields = new ArrayList<String>();
		fields.add("jsonschema");
		fields.add("version");

		List<Map<String, String>> results = doSelect(sql, fields);
		List<String> resultReturn = new ArrayList<String>();
		if (results.size() > 0) {
			Map<String, String> tmpRes = doSelect(sql, fields).get(0);
			resultReturn.add(tmpRes.get("jsonschema"));
			resultReturn.add(tmpRes.get("version"));
		}
		return resultReturn;

	}

	/**
	 * 
	 * @return
	 */
	public List<Map<String, String>> getSourceCatalogDetails() {
		String sql = "SELECT * from sourcecatalog";
		System.out.println(sql);
		List<String> fields = new ArrayList<String>();
		fields.add("source_key");
		fields.add("hivetablenameprefix");
		fields.add("inputfilelocation");
		fields.add("targethdfslocation");
		List<Map<String, String>> result = doSelect(sql, fields);
		return result;
	}
}
