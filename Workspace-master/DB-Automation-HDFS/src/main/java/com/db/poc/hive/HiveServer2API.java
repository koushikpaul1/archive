package com.db.poc.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Class to create connection with hive server 2.
 * @author anoop_khetan
 *
 */
public class HiveServer2API {
	private  String driverName = "org.apache.hive.jdbc.HiveDriver";
	private  String dbURLHIVE;
	private  String HIVE_USER;
	private  String HIVE_PASSWORD;
	
	public HiveServer2API(String MACHINE_IP, int MACHINE_PORT, String HIVE_DB, String HIVE_USER, String HIVE_PASSWORD) {		    
		    this.dbURLHIVE = "jdbc:hive2://"+ MACHINE_IP +":"+MACHINE_PORT +"/"+HIVE_DB;
	        this.HIVE_USER =  HIVE_USER;
	        this.HIVE_PASSWORD =  HIVE_PASSWORD;        
	       }
	
	/**
	 * create connection object
	 * @return
	 * 
	 * oracle_customer_1(SA_TRADING array<struct<CLOSE_DATE:string,INSTRUMENT_TYPE:string,UNITS:int,CREATED_DATE:string,EMPID:int,INSTRUMENT:string,ID:int,OPEN_DATE:string,CUSTOMER_ID:int,INDEXNAME:string>>) ROW FORMAT SERDE ''org.openx.data.jsonserde.JsonSerDe'' LOCATION ''/user/devbox/poc_target_directory/oracle_json/oracle_customer/1'''); 
Hive table def :create external table oracle_customer_1(SA_TRADING array<struct<CLOSE_DATE:string,INSTRUMENT_TYPE:string,UNITS:int,CREATED_DATE:string,EMPID:int,INSTRUMENT:string,ID:int,OPEN_DATE:string,CUSTOMER_ID:int,INDEXNAME:string>>) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION '/user/devbox/poc_target_directory/oracle_json/oracle_customer/1'
driverName  :org.apache.hive.jdbc.HiveDriver
16/03/07 14:09:31 INFO jdbc.Utils: Supplied authorities: fraitcf1vd1921.de.db.com:10000
16/03/07 14:09:31 INFO jdbc.Utils: Resolved authority: fraitcf1vd1921.de.db.com:10000
16/03/07 14:09:31 INFO jdbc.HiveConnection: Will try to open client transport with JDBC Uri: jdbc:hive2://fraitcf1vd1921.de.db.com:10000/oracle_json

	 */
	public Connection createConnection() {

		try {
			Class.forName(driverName);
			System.out.println("driverName  :" + driverName);
		} catch (ClassNotFoundException e) {
			System.out.print(" exception " + e);
		}
		Connection con = null;
		try {
			System.out.print(" dbURLHIVE " + dbURLHIVE);
			System.out.print(" HIVE_USER " + HIVE_USER);
			System.out.print(" HIVE_PASSWORD " + HIVE_PASSWORD);
		
			con = DriverManager.getConnection(dbURLHIVE, HIVE_USER, HIVE_PASSWORD);
			
	    	System.out.print(" connection obtained " + con);
			
		} catch (SQLException e) {
			System.out.print(" exception in getting connection " + e);
		}

		return con;
	}

	/**
	 * To run any query
	 * @param sql
	 * @return
	 */
	public boolean executeSQL(String sql) {
		Connection con = createConnection();
		
		System.out.println("got connection  executeSQL ");
		try {
			
			Statement stmt = con.createStatement();			
			System.out.println("firing command to create table : ");
			stmt.execute(sql);
		} catch (Exception e) {
			System.out.println(" excetion in  executeSQL "+e);
			return false;
		}
		return true;

	}
}