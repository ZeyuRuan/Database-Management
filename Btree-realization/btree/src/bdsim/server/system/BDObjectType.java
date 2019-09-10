package bdsim.server.system;

/**
 * @author dclee
 * @revision $Id: BDObjectType.java 192 2006-05-14 21:57:53 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public enum BDObjectType {
	INTEGER, FLOAT, STRING, DOUBLE;

	public static BDObjectType convertTo(String string) {
		if (string.equals("INTEGER")) {
			return INTEGER;
		} else if (string.equals("FLOAT")) {
			return FLOAT;
		} else if (string.equals("STRING")) {
			return STRING;
		} else if (string.equals("DOUBLE")) {
			return DOUBLE;
		}
		return null;
	}
};
