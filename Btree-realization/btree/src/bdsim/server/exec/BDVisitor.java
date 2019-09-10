package bdsim.server.exec;

import bdsim.server.exec.nodes.BDCondOpType;
import bdsim.server.exec.nodes.BDJoinType;
import bdsim.server.exec.nodes.NodeCondition;

/**
 * Defines the interface between the parsed abstract syntax tree and a visiting
 * class. A call to the visit() method of the root of the tree will generate a
 * series of these callbacks which correspond to the parsed SQL expression.
 * 
 * @author wpijewsk
 * @revision $Id: BDVisitor.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public interface BDVisitor {

	void handleAssignment(String column, Object value);

	void handleBoolOp(boolean isAnd);

	void handleColumnDecl(String column, String identifier);

	void handleColumnDeclStar();

	void handleCondition(NodeCondition condition);

	void handleConditionValueColumn(String column);

	void handleConditionValueLiteral(String string);

	void handleConditionValueNumber(double number);

	void handleCondOp(BDCondOpType eq);

	void handleDelete(boolean allRows, String table);

	void handleInsert(String table);

	void handleLiteral(String m_literal);

	void handleNumber(double m_number);

	void handleOrderBy(String column, boolean asc);

	void handleSelect();

	void handleTable(String table, String identifier, BDJoinType m_jointype);

	void handleUpdate(String table);
}
