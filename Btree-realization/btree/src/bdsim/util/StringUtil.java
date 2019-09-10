package bdsim.util;

import java.util.List;

public class StringUtil {

	public static String join(List<String> strings) {
		return join(strings, ", ");
	}
		
	public static String join(List<String> strings, String delimiter) {
		StringBuilder result = new StringBuilder();
		int ii;
		for (ii = 0; ii < strings.size() - 1; ii++) {
			result.append(strings.get(ii));
			result.append(delimiter);
		}
		if (strings.size() > 0) {
			result.append(strings.get(ii));
		}
		return result.toString();
	}
	
}
