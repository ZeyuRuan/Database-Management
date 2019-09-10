package bdsim.server.exec;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import bdsim.server.exec.BDCondition.ConditionValueType;
import bdsim.server.exec.BDSemanticVisitor.BDNameMappings;
import bdsim.server.exec.nodes.BDCondOpType;
import bdsim.server.exec.nodes.BDJoinType;
import bdsim.server.exec.nodes.Node;
import bdsim.server.exec.nodes.NodeCondition;
import bdsim.server.system.BDSystem;

/**
 * A plan to be executed by the database system. Contains relevant information
 * about the query so the system can execute it correctly.
 * 
 * @author wpijewsk
 * @revision $Id: BDPlan.java 257 2007-01-19 18:52:11 +0000 (Fri, 19 Jan 2007) wpijewsk $
 */
public final class BDPlan {

	// TODO Put algorithms to use in here
	// FIXME I don't capture the update data correctly. I need to
	// maybe add more callbacks to the BDVisitor interface.
	// TODO Support multi-query execution

	public class BDPlanException extends Exception {
		private static final long serialVersionUID = -5097023278978099980L;

		public BDPlanException(String string) {
			super(string);
		}
	}

	private final class PlannerVisitor implements BDVisitor {

		private int m_dataSeen;
		private BDCondition m_lastCond;
		private BDConditionList m_lastCondList;

		PlannerVisitor() {
			m_lastCond = null;
			m_lastCondList = null;
			m_dataSeen = 0;
		}

		public void handleAssignment(String column, Object value) {
			logger.debug("handleAssignment(column=" + column + ", value:"
					+ value + ")");

			try {
				m_data.put(getTableAndColumnName(column), value);
				logger.debug("Mapping " + getTableAndColumnName(column) + " value");
			} catch (BDPlanException e) {
				logger.error("BDPlanException caught in handleAssignment():");
				e.printStackTrace();
			}
		}

		public void handleBoolOp(boolean isAnd) {
			logger.debug("handleBoolOp(isAnd=" + isAnd + ")");

			m_lastCondList.setAndedTogether(isAnd);
		}

		public void handleColumnDecl(String column, String identifier) {
			logger.debug("handleColumnDecl(column:" + column + ", ident="
					+ identifier + ")");
			try {
				if (m_qtype == BDQueryType.SELECT) {
					m_selectAll = false;
					m_columnPairs.addLast(getTableAndColumnName(column));
				} else if (m_qtype == BDQueryType.INSERT) {
					m_columnPairs.addLast(getTableAndColumnName(column));
				}
			} catch (BDPlanException e) {
				logger.error("BDPlanException caught in handleColumnDecl():");
				e.printStackTrace();
			}
		}

		public void handleColumnDeclStar() {
			logger.debug("handleColumnDeclStar()");
			m_selectAll = true;
			assert (m_qtype == BDQueryType.SELECT || m_qtype == BDQueryType.DELETE);
		}

		public void handleCondition(NodeCondition condition) {
			logger.debug("handleCondition()");

			BDCondition newCond = new BDCondition(null, null, null);
			BDConditionList newList = new BDConditionList(newCond, true, null);

			if (m_conditions == null) {
				m_conditions = newList;
			} else {
				m_lastCondList.setRest(newList);
			}

			m_lastCond = newCond;
			m_lastCondList = newList;
		}

		public void handleConditionValueColumn(String column) {
			logger.debug("handleConditionValueColumn()");
			try {
				if (m_lastCond.getLhs() == null) {
					m_lastCond.setLhs(getTableAndColumnName(column),
							ConditionValueType.COLUMN);
				} else {
					m_lastCond.setRhs(getTableAndColumnName(column),
							ConditionValueType.COLUMN);
				}
			} catch (BDPlanException e) {
				logger
						.error("BDPlanException caught in handleColumnValueColumn():");
				e.printStackTrace();
			}
		}

		public void handleConditionValueLiteral(String literal) {
			logger.debug("handleConditionValueLiteral()");

			if (m_lastCond.getLhs() == null) {
				m_lastCond.setLhs(literal, ConditionValueType.LITERAL);
			} else {
				m_lastCond.setRhs(literal, ConditionValueType.LITERAL);
			}
		}

		public void handleConditionValueNumber(double number) {
			logger.debug("handleConditionValueNumber()");

			if (m_lastCond.getLhs() == null) {
				m_lastCond
						.setLhs(new Double(number), ConditionValueType.NUMBER);
			} else {
				m_lastCond
						.setRhs(new Double(number), ConditionValueType.NUMBER);
			}
		}

		public void handleCondOp(BDCondOpType eq) {
			logger.debug("handleCondOp()");

			m_lastCond.setCondOp(eq);
		}

		public void handleDelete(boolean allRows, String table) {
			logger.debug("handleDelete(allRows=" + allRows + ",table=" + table
					+ ")");
			m_qtype = BDQueryType.DELETE;

			if (allRows) {
				m_selectAll = true;
			}

			m_tables.addLast(table);
		}

		public void handleInsert(String table) {
			logger.debug("handleInsert(table:" + table + ")");
			m_qtype = BDQueryType.INSERT;
			m_tables.addLast(table);
			m_selectAll = false;
		}

		public void handleLiteral(String literal) {
			logger.debug("handleLiteral(literal:" + literal + ")");

			if (m_qtype == BDQueryType.INSERT) {

				if (m_columnPairs.isEmpty()) {
					String tableName = m_tables.get(0);

					for (String s : BDSystem.tableManager.getTableByName(
							tableName).getSchema().getNames()) {
						m_columnPairs.addLast(new BDTableColumnPair(tableName,
								s));
					}
				}

				logger.debug("Mapping " + m_columnPairs.get(m_dataSeen) + " to " + literal);
				m_data.put(m_columnPairs.get(m_dataSeen), literal);
				m_dataSeen++;
			}
		}

		public void handleNumber(double number) {
			logger.debug("handleNumber(number:" + number + ")");

			if (m_qtype == BDQueryType.INSERT) {

				if (m_columnPairs.isEmpty()) {
					String tableName = m_tables.get(0);

					for (String s : BDSystem.tableManager.getTableByName(
							tableName).getSchema().getNames()) {
						m_columnPairs.addLast(new BDTableColumnPair(tableName,
								s));
					}
				}

				logger.debug("Mapping " + m_columnPairs.get(m_dataSeen) + " to " + number);
				m_data.put(m_columnPairs.get(m_dataSeen), new Double(number));
				m_dataSeen++;
			}
		}

		public void handleOrderBy(String column, boolean asc) {
			logger.debug("handleOrderBy(string:" + column + ", asc: " + asc
					+ ")");

			try {
				m_orderByList.addLast(new BDOrderByInfo(
						getTableAndColumnName(column), asc));
			} catch (BDPlanException e) {
				logger.error("BDPlanException caught in handleOrderBy():");
				e.printStackTrace();
			}
		}

		public void handleSelect() {
			m_qtype = BDQueryType.SELECT;
			logger.debug("handleSelect()");
		}

		public void handleTable(String table, String identifier, BDJoinType type) {
			logger.debug("handleTable(table:" + table + ", ident=" + identifier
					+ ")");

			m_tables.add(table);

			// TODO Do something with the BDJoinType argument here.
		}

		public void handleUpdate(String table) {
			m_qtype = BDQueryType.UPDATE;
			logger.debug("handleUpdate(table:" + table + ")");
			
			m_tables.addLast(table);
		}
	}

	static Logger logger = Logger.getLogger(BDPlan.class);

	/**
	 * A list of columns. In a SELECT query, the list of columns to return, in
	 * INSERT, the list of columns to use.
	 */
	protected LinkedList<BDTableColumnPair> m_columnPairs;

	/**
	 * The list of conditions in a WHERE clause.
	 */
	protected BDConditionList m_conditions;

	/**
	 * The data to use in UPDATE and INSERT operations. A mapping from field
	 * name to value for that field.
	 */
	protected Map<BDTableColumnPair, Object> m_data;

	/**
	 * Unique transaction ID for this plan
	 */
	private int m_id;

	/**
	 * The list of joins in a SELECT query. These represent only explicit joins,
	 * where the qury is something like 'FROM table1 INNER JOIN table2'. If not
	 * a SELECT query, this should be null. The order of this list determines
	 * the order in which the joins should be performed.
	 */
	protected LinkedList<BDJoinInfo> m_joins;

	/**
	 * The name mappings used to check the names of the columns.
	 */
	private BDNameMappings m_mappings;

	/**
	 * The parsed node from which this node is constructed.
	 */
	private Node m_node;

	/**
	 * The order by list for a SELECT.
	 */
	protected LinkedList<BDOrderByInfo> m_orderByList;

	/**
	 * The type of query.  One of {SELECT, INSERT, UPDATE, DELETE}.
	 */
	protected BDQueryType m_qtype;

	/**
	 * Whether or not the user has selected all fields.
	 */
	protected boolean m_selectAll;

	/**
	 * A list of tables 
	 */
	protected LinkedList<String> m_tables;

	/**
	 * Class constructor.
	 * 
	 * @param tasks
	 *            The tasks to add to this plan
	 * @param nameMappings
	 *            The name mappings used in this query
	 */
	public BDPlan(Node node, BDNameMappings nameMappings) {
		m_node = node;
		m_columnPairs = new LinkedList<BDTableColumnPair>();
		m_data = new Hashtable<BDTableColumnPair, Object>();
		m_joins = new LinkedList<BDJoinInfo>();
		m_orderByList = new LinkedList<BDOrderByInfo>();
		m_tables = new LinkedList<String>();
		m_mappings = nameMappings;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object other) {
		if (!(other instanceof BDPlan)) {
			return false;
		}
		BDPlan otherPlan = (BDPlan) other;

		boolean colPairsEqual = m_columnPairs.equals(otherPlan.m_columnPairs);

		boolean condsEqual = false;
		if (m_conditions == null) {
			condsEqual = (otherPlan.m_conditions == null);
		} else {
			condsEqual = m_conditions.equals(otherPlan.m_conditions);
		}

		boolean dataEqual = true;
		if (m_data == null) {
			dataEqual = (otherPlan.m_data == null);
		} else {
			for (BDTableColumnPair pair : m_data.keySet()) {
				Object myValue = this.m_data.get(pair);
				Object otherValue = otherPlan.m_data.get(pair);

				logger.debug("Does otherPlan.m_data contain the key?: "
						+ otherPlan.m_data.containsKey(pair));
				// FIXME otherValue is always null, which is weird
				
				dataEqual = dataEqual && myValue.equals(otherValue);
			}
		}

		boolean joinsEqual = m_joins.equals(otherPlan.m_joins);

		boolean mappingsEqual = false;
		if (this.m_mappings == null) {
			mappingsEqual = (otherPlan.m_mappings == null);
		} else {
			mappingsEqual = this.m_mappings.equals(otherPlan.m_mappings);
		}

		boolean orderByEqual = this.m_orderByList
				.equals(otherPlan.m_orderByList);

		boolean qTypeEqual = this.m_qtype == otherPlan.m_qtype;

		boolean selectAllEqual = this.m_selectAll == otherPlan.m_selectAll;

		boolean tablesEqual = this.m_tables.equals(otherPlan.m_tables);

		return colPairsEqual && condsEqual && dataEqual && joinsEqual
				&& mappingsEqual && orderByEqual && qTypeEqual
				&& selectAllEqual && tablesEqual;
	}

	/**
	 * Calls into the PlannerVisitor to visit each node of the abstract syntax
	 * tree, convert that representation into a BDPlan
	 */
	public void generatePlan() {
		PlannerVisitor visitor = new PlannerVisitor();
		m_node.visit(visitor);
	}

	public List<BDTableColumnPair> getColumnPairs() {
		return m_columnPairs;
	}

	public BDConditionList getConditions() {
		return m_conditions;
	}

	public Map<BDTableColumnPair, Object> getData() {
		return m_data;
	}

	public int getId() {
		return m_id;
	}

	public List<BDJoinInfo> getJoins() {
		return m_joins;
	}

	public List<BDOrderByInfo> getOrderByList() {
		return m_orderByList;
	}

	public BDQueryType getQTtype() {
		return m_qtype;
	}

	/**
	 * Takes in a column expression and returns the full table name of that
	 * column expression. Uses the name mappings generated in the
	 * <code>BDSemanticVisitor</code> to resolve all the abbreviations.
	 * 
	 * @param colExpr
	 *            A column expression (like a.id, id, or members.id)
	 * @return The full table column tuple that identifies that column
	 * @throws BDPlanException
	 */
	private BDTableColumnPair getTableAndColumnName(String colExpr)
			throws BDPlanException {
		String tblName = null;
		String colName = null;

		if (colExpr.contains(".")) {
			// BDTable name or abbreviation is specified
			String tblIdent = colExpr.substring(0, colExpr.indexOf('.'));
			colName = colExpr.substring(colExpr.indexOf('.') + 1);
			if (m_mappings.getTblAbbrs().values().contains(tblIdent)) {
				tblName = tblIdent;
			} else if (m_mappings.getTblAbbrs().keySet().contains(tblIdent)) {
				tblName = m_mappings.getTblAbbrs().get(tblIdent);
			} else {
				throw new BDPlanException(
						"Illegal table abbreviation encountered: " + colExpr);
			}
		} else {
			// No table name specified - just column name only
			int numOccurences = 0;
			for (String s : m_mappings.getTblAbbrs().values()) {
				if (BDSystem.tableManager.getTableByName(s).getSchema()
						.isField(colExpr)) {
					numOccurences++;
					tblName = s;
				}
			}
			if (numOccurences == 0) {
				throw new BDPlanException("Column " + colExpr
						+ " does not appear in the relevant tables");
			} else if (numOccurences > 1) {
				throw new BDPlanException("Column " + colExpr + " is ambiguous");
			} else {
				colName = colExpr;
			}
		}

		if (BDSystem.tableManager.getTableByName(tblName) == null) {
			throw new BDPlanException("null table name retrieved");
		}

		return new BDTableColumnPair(tblName, colName);
	}

	public List<String> getTables() {
		return m_tables;
	}

	public boolean isSelectAll() {
		return m_selectAll;
	}

	public void setId(int m_id) {
		this.m_id = m_id;
	}

	public String toString() {
		String msg = "BDPlan:\nQueryType: " + m_qtype + "\nTables:";
		for (String s : m_tables) {
			msg += s + ", ";
		}

		// TODO

		return msg;
	}
}
