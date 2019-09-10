package bdsim.server.system.handler;

import java.util.List;
import java.util.LinkedList;

import bdsim.server.exec.BDTableColumnPair;
import bdsim.server.exec.nodes.BDCondOpType;
import bdsim.server.system.index.BDIndex.RangeType;

/**
 * @author dclee
 */
public class BDHandlerFunctions {

	public static RangeType condToRange(BDCondOpType c) {
		switch (c) {
		case EQ:
			return RangeType.EQ;
		case NEQ:
			return RangeType.NEQ;
		case LT:
			return RangeType.LT;
		case GT:
			return RangeType.GT;
		case LTEQ:
			return RangeType.LTEQ;
		case GTEQ:
			return RangeType.GTEQ;
		case LIKE:
			return RangeType.LIKE;
		}
		return null;
	}

	public static RangeType invertRange(RangeType r) {
		switch (r) {
		case LT:
			return RangeType.GT;
		case GT:
			return RangeType.LT;
		case GTEQ:
			return RangeType.LTEQ;
		case LTEQ:
			return RangeType.GTEQ;
		default:
			return r;
		}
	}

	public static List<String> stripTables(List<BDTableColumnPair> tcpList) {
		List<String> columns = new LinkedList<String>();
		for (BDTableColumnPair tcp : tcpList) {
			columns.add(tcp.getColumn());
		}
		return columns;
	}
}
