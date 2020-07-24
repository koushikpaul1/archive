package com.db.hive;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.db.poc.hive.HiveServer2API;

public class HiveServer2APITest {

	@Test
	public void test() {

		String tableName = "TEST";
		// Create table
		String sql = "create table " + tableName + " (SA_TRADING array<struct<CLOSE_DATE:string,INSTRUMENT_TYPE:string,"
				+ "UNITS:float,CREATED_DATE:string,EMPID:int,INSTRUMENT:string,ID:int,OPEN_DATE:string,CUSTOMER_ID:int,INDEXNAME:string>>) "
				+ " ROW FORMAT SERDE " + "'org.openx.data.jsonserde.JsonSerDe'" + " LOCATION "
				+ "'/user/Kartik_Bhatnagar/oracle_trading'";
		System.out.println("Create sql: " + sql);
	}

	@Test
	public void createOrDeleteTable() throws SQLException {

		HiveServer2API hs = new HiveServer2API(null, 0, null, null, null);

		// delete table
		String tableName = "oracle_trading_Feb05";
		hs.executeSQL("drop table " + tableName);

		// Create table
		String sql = "create table " + tableName + " (SA_TRADING array<struct<CLOSE_DATE:string,INSTRUMENT_TYPE:string,"
				+ "UNITS:float,CREATED_DATE:string,EMPID:int,INSTRUMENT:string,ID:int,OPEN_DATE:string,CUSTOMER_ID:int,INDEXNAME:string>>) "
				+ " ROW FORMAT SERDE " + "'org.openx.data.jsonserde.JsonSerDe'" + " LOCATION "
				+ "'/user/Kartik_Bhatnagar/oracle_trading'";
		System.out.println("Create sql: " + sql);
		hs.executeSQL(sql);

		// Describe table
		sql = "describe " + tableName;
		System.out.println("Schema sql: " + sql);
		ResultSet res = hs.createConnection().createStatement().executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
		}

	}

}