package com.poc.json;

import org.junit.Test;

import com.db.poc.json.FileHandler;
import com.db.poc.json.HiveJSONSerdeExternalTableGenerator;

public class HiveJSONSerdeExternalTableGeneratorTest {

	@Test
	public void externaltable_complex() throws Exception {

		String root = "C:/Users/kartik_bhatnagar/workspace";
		String[] args = new String[1];
		args[0] = root + "/DB-Automation/src/main/resources/1_Hive_external_table.table";
		HiveJSONSerdeExternalTableGenerator jsonSchemaGen = new HiveJSONSerdeExternalTableGenerator();

		FileHandler fileHandler = new FileHandler();
		String jsonSchema = fileHandler.readOneLine(args[0]);

		String tableDef = jsonSchemaGen.generateDefinition(jsonSchema, "oracle_trading",
				"/user/Kartik_Bhatnagar/oracle_employee");
		System.out.println("----------- Generated table definition --------- ");
		System.out.println(tableDef);
	}

	@Test
	public void isNumber() throws Exception {

		System.out.println("22.2 isfloat " + isFloat("22.2"));
		System.out.println("22 isfloat   " + isFloat("22"));
		System.out.println("22 is int " + isInt("22"));
		System.out.println("22.22 is int " + isInt("22.22"));

		// first check isInt --> false--> isFloat
		// isInt --> true use int

	}

	boolean isFloat(String str) {
		try {
			Float.parseFloat(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	boolean isInt(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

}
