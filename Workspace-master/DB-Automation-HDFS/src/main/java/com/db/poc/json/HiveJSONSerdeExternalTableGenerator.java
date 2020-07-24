package com.db.poc.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.db.poc.exception.ExternalHiveTableGeneratorException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class HiveJSONSerdeExternalTableGenerator extends CommonJSONUtil {

	private static JsonFactory factory = null;
	private static ObjectMapper mapper = new ObjectMapper(factory);

	public String generateDefinition(String json, String tableName, String hdfslocation) {

		System.out.println(" input json " + json);
		ObjectNode obj2;
		try {
			obj2 = mapper.readValue(json, ObjectNode.class);
		} catch (IOException e) {
			throw new ExternalHiveTableGeneratorException("Unable to parse JSON");
		}
		List<String> keys = iteratorToList(obj2.fieldNames());
		System.out.println(" keys..... " + keys);
		List<String> results = null;
		StringBuilder sb = new StringBuilder();
		// start table building
		sb.append("create external table" + " " + tableName);
		sb.append("(");
		//sb.append(System.lineSeparator());
		int sizeofKeys = keys.size();
		int keyCounter = 1;
		System.out.println(" sizeofKeys " + sizeofKeys);
		for (String key : keys) {
			sb.append(key);
			sb.append(" ");
			List<String> addPartOfTableDefinition = new ArrayList<String>();
			List<String> isDone = new ArrayList<String>();
			// do for loop, take one value at time.
			// using recursion go in depth, once hit bottom, simple return.
			// add key, return full value, add comma if not last.
			try {
				results = generateTableDef(obj2.get(key).toString(), addPartOfTableDefinition, isDone);
			} catch (IOException e) {
				throw new ExternalHiveTableGeneratorException("Error while generating table definition. ");
			}
			// To remove space before ">"
			for (int resultIndex = 0; resultIndex < results.size(); resultIndex++) {
				
				sb.append(results.get(resultIndex));
				
				/*if (((resultIndex + 1) < results.size()) && (">").equals(results.get(resultIndex + 1))) {
					if (!(System.lineSeparator()).equals(results.get(resultIndex))) {
						sb.append(results.get(resultIndex));
					}
				} else {
					sb.append(results.get(resultIndex));
				}*/			
			}

			if (sizeofKeys != 1) {
				if (keyCounter != sizeofKeys) {
					sb.append(",");
					//sb.append(System.lineSeparator());
				}
			}
			keyCounter++;

		} // for loop keys
		//sb.append(System.lineSeparator());
		sb.append(")");
		//sb.append(System.lineSeparator());
		sb.append(" ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'");
		//sb.append(System.lineSeparator());
		// sb.append("LOCATION '/user/Kartik_Bhatnagar/oracle_employee'");
		sb.append(" LOCATION '" + hdfslocation + "'");
		System.out.println("\n");
		return sb.toString();
	}

	/**
	 * 
	 * @param jsonValue
	 * @param addPartOfTableDefinition
	 * @param isDone
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	private List<String> generateTableDef(String jsonValue, List<String> addPartOfTableDefinition, List<String> isDone)
			throws JsonParseException, JsonMappingException, IOException {

		// if complex*, [] OR {}
		// if [] --> if {} OR simple --> if {} --> if complex* OR if all simple,
		// check and return.
		// if [] --> if all simple, check and return
		// don't know what to do //[{ "key": "333" },{ "a": "b"},{ "a": [{ "a":
		// "c" }] {valid JSON, invalid use}
		// else if All simple, check and return

		// if complex*, [] OR {}
		if (isObject(jsonValue) && isDone.size() == 0) {
			addPartOfTableDefinition.add("struct<");
			//addPartOfTableDefinition.add(System.lineSeparator());
			ObjectNode obj2 = mapper.readValue(jsonValue, ObjectNode.class);
			// for loop for all key/value
			List<String> keys = iteratorToList(obj2.fieldNames());
			int totalKeysLength = keys.size();
			for (int index = 0; index < totalKeysLength; index++) {
				// check the nature of value.
				String value = obj2.get(keys.get(index)).toString();
				boolean isStringValue = isNotCurlyOrSqureTypeValue(value);
				if (isStringValue) {
				//	addPartOfTableDefinition.add(applyDataTypeOnKey(keys.get(index)));
				//	addPartOfTableDefinition.add(applyDataTypeOnKey(value));
					addPartOfTableDefinition.add(applyDataTypeOnKey(keys.get(index),value));
				} else {
					// call recursive function, it should generate right
					// definition. add it.
					addPartOfTableDefinition.addAll(generateTableDef(value, new ArrayList<String>(), isDone));
				}
				// for , arrangement
				if (totalKeysLength != 1 && index < (totalKeysLength - 1)) {
					addPartOfTableDefinition.add(",");
				}
				//addPartOfTableDefinition.add(System.lineSeparator());
			}
			addPartOfTableDefinition.add(">");
			isDone.add("return");
			return addPartOfTableDefinition;

			// [] type
		} else if (isArray(jsonValue) && isDone.size() == 0) {
			addPartOfTableDefinition.add("array<");
			ArrayNode obj2 = mapper.readValue(jsonValue, ArrayNode.class);
			String tmp = obj2.get(0).toString();
			// {} with in array
			if (isObject(tmp)) {
				int sizeOfArray = obj2.size();
				// different types of {} within array
				for (int index = 0; index < obj2.size(); index++) {
					isDone.clear();
					addPartOfTableDefinition
							.addAll(generateTableDef(obj2.get(index).toString(), new ArrayList<String>(), isDone));
					// add ,
					if (sizeOfArray != 1 && index < (sizeOfArray - 1)) {
						addPartOfTableDefinition.add(",");
					}
					//addPartOfTableDefinition.add(System.lineSeparator());
				}
			} else if (isNotCurlyOrSqureTypeValue(tmp)) {
				if (isInt(jsonValue)) {
					addPartOfTableDefinition.add("int");
				} else if (isFloat(jsonValue)) {
					addPartOfTableDefinition.add("float");
				} else {
					addPartOfTableDefinition.add("string");
				}
			}
			addPartOfTableDefinition.add(">");
			return addPartOfTableDefinition;
			// plain value
		} else if (isNotCurlyOrSqureTypeValue(jsonValue) && isDone.size() == 0) {

			if (isInt(jsonValue)) {
				addPartOfTableDefinition.add("int");
			} else if (isFloat(jsonValue)) {
				addPartOfTableDefinition.add("float");
			} else {
				addPartOfTableDefinition.add("string");
			}
		}
		return addPartOfTableDefinition;
	}

	
	/**
	 * 
	 * @param key
	 * @return
	 */
	//private String applyDataTypeOnKey(String key) {
	private String applyDataTypeOnKey(String key,String value) {
		String withDataType = null;
	//	if (isInt(key)) {
		if (isInt(value)) {
			withDataType = key + ":" + "int";
	//	} else if (isFloat(key)) {
		} else if (isFloat(value)) {
			withDataType = key + ":" + "float";
		} else {
			withDataType = (key + ":" + "string");
		}
		return withDataType;
	}

}
