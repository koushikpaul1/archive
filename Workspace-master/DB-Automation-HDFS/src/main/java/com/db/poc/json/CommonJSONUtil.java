package com.db.poc.json;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CommonJSONUtil {

	private static JsonFactory factory = null;
	private static ObjectMapper mapper = new ObjectMapper(factory);

	
	protected List<String> iteratorToList(Iterator<String> keyValue) {
		List<String> copy = new ArrayList<String>();
		while (keyValue.hasNext())
			copy.add(keyValue.next());
		return copy;
	}

	protected boolean isFloat(String str) {
		try {
			Float.parseFloat(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	protected boolean isInt(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * Attempts to process value as a JSON LD object node.
	 * 
	 * @param jsonLDNode
	 * @param key
	 * @param value
	 * @return
	 */
	protected boolean isObject(final String value) {

		// attempt as object
		boolean isObject = value.matches("^\\{(.*?)\\}$");
		if (isObject) {
			try {
				mapper.readValue(value, ObjectNode.class);
			} catch (final Exception ex) {
				// no need to throw this
				isObject = false;
			}
		}
		return isObject;
	}

	/**
	 * Attempts to process as JSON Array node
	 * 
	 * @param jsonLDNode
	 * @param key
	 * @param value
	 * @return
	 */
	protected boolean isArray(final String value) {
		// attempt as array
		boolean isArray = value.matches("^\\[(.*?)\\]$");
		if (isArray) {
			try {
				mapper.readValue(value, ArrayNode.class);
			} catch (final Exception ex) {
				// no need to throw this exception
				isArray = false;
			}
		}
		return isArray;
	}

	protected boolean isNotCurlyOrSqureTypeValue(final String value) {
		// attempt as array
		boolean isString = !isObject(value) && !isArray(value);
		if (isString) {
			return true;
		}
		return false;
	}

}
