package edu.zuo.setree.JSON;

import java.util.List;

/**
 * Created by wangyifei on 2018/3/17.
 */
public class JSON {
	// value != null
	public static String toJson(String value) {
		if (value != null) {
			if (value.contains("\"")) {
				String temp = value.replace("\"", "\\\"");
				value = temp;
			}
			return "\"" + value + "\"";
		} else {
			return null;
		}
	}

	// key != null
	// like "AAA": "BBB"
	public static String toJson(String key, String value) {
		if (value != null) {
			return "\"" + key + "\": \"" + value + "\"";
		} else {
			return "\"" + key + "\": null";
		}
	}

	// key != null
	// like "AAA": 3
	public static String toJson(String key, int value) {
		return "\"" + key + "\": " + value;
	}

	// stringList != null
	// like {"AAA":"BBB","CCC":"DDD","EEE":"FFF"}
	public static String toJsonSet(List<String> stringList) {
		String str = null;
		for (int i = 0; i < stringList.size(); i++) {
			if (str != null) {
				str = str + ", " + stringList.get(i);
			} else {
				str = stringList.get(i);
			}
		}
		return "{" + str + "}";
	}

	// key != null
	// like "aaa":{"AAA":"BBB","CCC":"DDD","EEE":"FFF"}
	public static String toJsonSet(String key, List<String> stringList) {
		if (stringList == null) {
			return "{\"" + key + "\": null}";
		}
		String str = null;
		for (int i = 0; i < stringList.size(); i++) {
			if (str != null) {
				str = str + ", " + stringList.get(i);
			} else {
				str = stringList.get(i);
			}
		}
		return "{\"" + key + "\": {" + str + "}}";
	}

	// key != null
	// like "aaa":[{"AAA":"BBB"},{"AAA":"CCC"},{"AAA":"DDD"}]
	public static String toJsonArray(String key, List<String> stringList) {
		if (stringList == null)
			return "\"" + key + "\": null";

		String str = null;
		for (int i = 0; i < stringList.size(); i++) {
			if (str != null) {
				str = str + ", " + stringList.get(i);
			} else {
				str = stringList.get(i);
			}
		}
		return "\"" + key + "\": [" + str + "]";
	}
}
