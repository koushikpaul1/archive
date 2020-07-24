package com.poc.json;

import org.junit.Test;

import com.db.poc.json.FileHandler;
import com.db.poc.json.JsonToJsonSchemaCompare;

public class JsonToJsonSchemaCompareTest {

	@Test
	public void testCompareTwoJsons() throws Exception {
		String root = "C:/Users/kartik_bhatnagar/workspace";
		FileHandler fileHandler = new FileHandler();
		String inputJSON = fileHandler.readOneLine(root + "/DB-Automation/src/main/resources/1_input_json.json");
		String inputSchema = fileHandler.readOneLine(root + "/DB-Automation/src/main/resources/1_json_schema.json");
		JsonToJsonSchemaCompare jsonSchemaGen = new JsonToJsonSchemaCompare();
		boolean result = jsonSchemaGen.compareJSON(inputJSON, inputSchema);
		System.out.println("result from json compare " + result);
	}

}