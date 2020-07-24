package com.db.poc.json;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.db.poc.exception.JSONSchemaCompareException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonToJsonSchemaCompare extends CommonJSONUtil {

	private static JsonFactory factory = null;
	private static ObjectMapper mapper = new ObjectMapper(factory);

	public boolean compareJSON(String inputJSON, String inputSchema) {
		System.out.println(" input JSON...... " + inputJSON);
		System.out.println(" Schema JSON..... " + inputSchema);

		// get input json keys
		List<String> inputJsonkeys;
		// get input schema keys
		List<String> jsonSchemakeys;
		try {
			inputJsonkeys = getKeysObjectType(inputJSON);
			jsonSchemakeys = getKeysObjectType(inputSchema);
		} catch (IOException e) {
			throw new JSONSchemaCompareException("JSON parse error while checking keys");
		}
		// check if both keys are same
		compareKeys(inputJsonkeys, jsonSchemakeys);
		// to get values
		ObjectNode inputJsonObj;
		ObjectNode jsonSchemaObj;
		try {
			inputJsonObj = mapper.readValue(inputJSON, ObjectNode.class);
			jsonSchemaObj = mapper.readValue(inputSchema, ObjectNode.class);
		} catch (Exception e) {
			throw new JSONSchemaCompareException("JSON parse error while checking values.");
		}
		// compare jsons recursively.
		try {
			for (String key : inputJsonkeys) {
				compareJson2JsonSchema(inputJsonObj.get(key).toString(), jsonSchemaObj.get(key).toString());
			}
		} catch (Exception e) {
			throw new JSONSchemaCompareException(e);
		}

		return true;
	}

	/**
	 * compare two set of keys
	 * 
	 * @param inputJsonkeys
	 * @param jsonSchemakeys
	 * @return
	 */
	private boolean compareKeys(List<String> inputJsonkeys, List<String> jsonSchemakeys) {
		// compare if both keys are same. inputJsonkeys & jsonSchemakeys
		String keyNotMatched = foundUnmatchedKey(inputJsonkeys, jsonSchemakeys);
		if (keyNotMatched != null) {
			throw new JSONSchemaCompareException("Keys are not same. Schema key: " + keyNotMatched
					+ " not present input JSON Keys : " + inputJsonkeys);
		}
		keyNotMatched = foundUnmatchedKey(jsonSchemakeys, inputJsonkeys);
		if (keyNotMatched != null) {
			throw new JSONSchemaCompareException("Keys are not same. Input JSON key: " + keyNotMatched
					+ " not present in Schema Keys : " + jsonSchemakeys);
		}
		return true;
	}

	/**
	 * return key which is not matched.
	 * 
	 * @param inputJsonkeys
	 * @param jsonSchemakeys
	 * @return
	 */
	private String foundUnmatchedKey(List<String> inputJsonkeys, List<String> jsonSchemakeys) {
		for (String schema : jsonSchemakeys) {
			String match = schema;
			for (String input : inputJsonkeys) {
				if (schema.equals(input)) {
					match = null;
				}
			}
			// give the key which is not match.
			if (match != null) {
				return match;
			}
		}
		// return null, all matched
		return null;
	}

	/**
	 * compare JSON to JSON
	 * 
	 * @param valueJson
	 * @param valueSchema
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 * @throws JSONSchemaCompareException
	 */
	private boolean compareJson2JsonSchema(String valueJson, String valueSchema)
			throws JsonParseException, JsonMappingException, IOException, JSONSchemaCompareException {

		// if complex*, [] OR {}
		if (isObject(valueJson) && isObject(valueSchema)) {
			ObjectNode valueObject2 = mapper.readValue(valueJson, ObjectNode.class);
			ObjectNode schemaObject2 = mapper.readValue(valueSchema, ObjectNode.class);
			// get keys
			List<String> valueKeys2 = iteratorToList(valueObject2.fieldNames());
			List<String> schemaKeys2 = iteratorToList(schemaObject2.fieldNames());
			// compare two list here valueKeys & schemaKeys.
			// {} [] ---> {key/object another complex type} , [ {},]
			compareKeys(valueKeys2, schemaKeys2);
			// key loop
			for (String key : schemaKeys2) {
				// check the nature of value.
				String valSchema1 = schemaObject2.get(key).toString();
				String valJSON1 = valueObject2.get(key).toString();
				// check value data type of valJSON & valSchema.
				// check if same data type and non complex type.
				if (isNotCurlyOrSqureTypeValue(valSchema1) && isNotCurlyOrSqureTypeValue(valJSON1)) {
					// if data type same return true else false.
					if (!checkDataTypes(valJSON1, valSchema1)) {
						throw new JSONSchemaCompareException("Data types are not same. input: " + valJSON1
								+ " schema : " + valSchema1 + " for key " + key);
					}
				} else {
					// {} or [] value, recursive.
					try {
						compareJson2JsonSchema(valJSON1, valSchema1);
					} catch (JSONSchemaCompareException exe) {
						throw new JSONSchemaCompareException(exe);
					}
				}
			}
			// [] style
		} else if (isArray(valueJson) && isArray(valueSchema)) {
			String tmpSchema = mapper.readValue(valueSchema, ArrayNode.class).get(0).toString();
			String tmpValue = mapper.readValue(valueJson, ArrayNode.class).get(0).toString();
			// let assume array has one type of collection so checking zero
			if (isObject(tmpSchema) && isObject(tmpValue)) {
				try {
					// recursive call.
					compareJson2JsonSchema(tmpValue, tmpSchema);
				} catch (JSONSchemaCompareException exe) {
					throw new JSONSchemaCompareException(exe);
				}
				// simple type value
			} else if (isNotCurlyOrSqureTypeValue(tmpValue) && isNotCurlyOrSqureTypeValue(tmpSchema)) {
				try {
					
					compareJson2JsonSchema(tmpValue, tmpSchema);
				} catch (JSONSchemaCompareException exe) {
					throw new JSONSchemaCompareException(exe);
				}
			} else {
				throw new JSONSchemaCompareException(
						"Data types are not same. input: " + tmpValue + " schema : " + tmpSchema);
			}
			// simple type
		} else if (isNotCurlyOrSqureTypeValue(valueJson) && isNotCurlyOrSqureTypeValue(valueSchema)) {
			// check if data types are same
			if (!checkDataTypes(valueJson, valueSchema)) {
				throw new JSONSchemaCompareException(
						"Data types are not same. input: " + valueJson + " schema : " + valueSchema);
			}
		} else {
			throw new JSONSchemaCompareException(
					"Not matching schema for values. Input: " + valueJson + " schema : " + valueSchema);

		}
		return true;
	}

	/**
	 * check if both data types are same.
	 * 
	 * @param valJSON1
	 * @param valSchema1
	 * @return
	 */
	private boolean checkDataTypes(String valJSON1, String valSchema1) {
		if (isInt(valSchema1)) {
			if (isInt(valJSON1)) {
				return true;
			} else {
				return false;
			}
		} else if (isFloat(valSchema1)) {
			if (isFloat(valJSON1)) {
				return true;
			} else {
				return false;
			}
		} else {
			if (isFloat(valJSON1) || isInt(valJSON1)) {
				return false;
			}
			return true;
		}

	}

	/**
	 * 
	 * get all keys of object.
	 * 
	 * @param input
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	private List<String> getKeysObjectType(String input) throws JsonParseException, JsonMappingException, IOException {
		ObjectNode inputJsonObj = mapper.readValue(input, ObjectNode.class);
		Iterator<String> inputJsonkeysItr = inputJsonObj.fieldNames();
		List<String> inputJsonkeys = iteratorToList(inputJsonkeysItr);
		return inputJsonkeys;
	}

}