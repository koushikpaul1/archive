package com.db.sql;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.db.poc.sql.DBAPIService;

public class DBAPIServiceTest {

	@Test 
	public void insertFirstRecordTest(){
		DBAPIService mySQL = new DBAPIService(null,null, 0, null, null, null);
	    mySQL.insertFirstRecordSchemaCatalog("abc", "jsonSchema1", 1,"hive table name" ,"system generated  ");
		
	}
	
	
	private String currentDateInFormat(){		
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
	    Date now = new Date();
	    String strDate = sdfDate.format(now);
	    return strDate;
	  //  System.out.println(" strDate "+strDate);
	}
	
	
	@Test 
	public void updateEndDateTest(){
		DBAPIService mySQL = new DBAPIService(null,null, 0, null, null, null);
		//oracle-sa_customer
	    mySQL.updateEndDateSchemaCatalog("oracle-sa_customer", currentDateInFormat(),"new colum added",1);
	}
	
	

}
