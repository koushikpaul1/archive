package com.db.poc.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.db.poc.run.DBAutomationConstant;

/**
 * 
 * @author Kartik_Bhatnagar
 *
 */
public class SQLDBAPI {

	private String DB_OPTION;
	private String DBSQL_URL;
	private String DB_USER;
	private String DB_PASS;

	/*
	 * private String dbURLH2 =
	 * "jdbc:h2:tcp://localhost/my-db/test;DATABASE_TO_UPPER=false"; private
	 * String H2_USER = "sa"; private String H2_PASS = "sa";
	 */
	public SQLDBAPI(String DB_OPTION, String MACHINE_IP, int MACHINE_PORT, String DBName, String DB_USER,
			String DB_PASS) {
		super();

		this.DB_OPTION = DB_OPTION;
		if (DB_OPTION.equalsIgnoreCase(DBAutomationConstant.MYSQL_DB_OPTION)) {
			this.DBSQL_URL = "jdbc:mysql://" + MACHINE_IP + ":" + MACHINE_PORT + "/" + DBName;
		} else if (DB_OPTION.equalsIgnoreCase(DBAutomationConstant.POSTGRES_DB_OPTION)) {
			this.DBSQL_URL = "jdbc:postgresql://" + MACHINE_IP + ":" + MACHINE_PORT + "/" + DBName;
		} else if (DB_OPTION.equalsIgnoreCase(DBAutomationConstant.H2_DB_OPTION)) {
			this.DBSQL_URL = "jdbc:h2:tcp://" + MACHINE_IP + "/h2-db/" + DBName + ";DATABASE_TO_UPPER=false";
		}
		this.DB_USER = DB_USER;
		this.DB_PASS = DB_PASS;
	}

	protected void runSqlCommand(String sql) {
		Statement stmt = null;
		Connection conn = createConnection();
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			System.out.println(" error in catch runSqlCommand " + e);
		} finally {
			try {

				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				System.out.println(" error in finally  runSqlCommand" + e);
			}

		}
	}

	protected List<Map<String, String>> doSelect(String sql, List<String> fields) {
		Statement stmt = null;
		ResultSet result = null;
		List<Map<String, String>> allLists = new ArrayList<Map<String, String>>();
		Connection conn = createConnection();
		try {
			stmt = conn.createStatement();
			result = stmt.executeQuery(sql);
			while (result.next()) {
				Map<String, String> valueResult = new HashMap<String, String>();
				for (String field : fields) {
					valueResult.put(field, result.getString(field));
				}
				allLists.add(valueResult);
			}
			return allLists;
		} catch (SQLException e) {
			System.out.println(" doSelect " + e);
			return allLists;
		} finally {
			try {
				if (result != null)
					result.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				System.out.println(" error in finally  doSelect" + e);
			}
		}
	}

	protected Map<String, List<String>> doSelectMap(String sql, List<String> fields) {
		Statement stmt = null;
		ResultSet result = null;
		Map<String, List<String>> allLists = new HashMap<String, List<String>>();
		Connection conn = createConnection();
		try {
			stmt = conn.createStatement();
			result = stmt.executeQuery(sql);
			while (result.next()) {
				List<String> valueResult = new ArrayList<String>();
				String key = null;
				for (String field : fields) {
					valueResult.add(result.getString(field));
					key = result.getString("key");
				}
				allLists.put(key, valueResult);
			}
			return allLists;
		} catch (SQLException e) {
			System.out.println(" doSelect " + e);
			return allLists;
		} finally {
			try {
				if (result != null)
					result.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				System.out.println(" error in finally  doSelect" + e);
			}
		}
	}

	public Connection createConnection() {
		Connection conn = null;
		try {
			if (DB_OPTION.equalsIgnoreCase(DBAutomationConstant.MYSQL_DB_OPTION)) {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
			} else if (DB_OPTION.equalsIgnoreCase(DBAutomationConstant.POSTGRES_DB_OPTION)) {
				Class.forName("org.postgresql.Driver").newInstance();
			} else if (DB_OPTION.equalsIgnoreCase(DBAutomationConstant.H2_DB_OPTION)) {
				Class.forName("org.h2.Driver").newInstance();
			}
			// Get a connection
			conn = DriverManager.getConnection(DBSQL_URL, DB_USER, DB_PASS);
		} catch (Exception except) {
			System.out.println(" createConnection error " + except);
		}
		return conn;
	}


}