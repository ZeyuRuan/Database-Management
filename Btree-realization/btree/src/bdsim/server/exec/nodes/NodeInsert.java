package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * Represents an INSERT statement
 * @author wpijewsk
 * @revision $Id: NodeInsert.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeInsert extends NodeStatement {

	private String m_table;
	private NodeIdList m_columns;
	private NodeValueList m_values;

	/**
	 * Class constructor.
	 * @param table
	 * @param columns
	 * @param values
	 */
	public NodeInsert(String table, NodeIdList columns, NodeValueList values) {
		this.m_table = table;
		this.m_columns = columns;
		this.m_values = values;
	}

	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleInsert(m_table);

		if (m_columns != null) {
			m_columns.visit(visitor);
		}

		if (m_values != null) {
			m_values.visit(visitor);
		}
	}

	@Override
	public boolean equals(Object other) {

		if (!(other instanceof NodeInsert)) {
			return false;
		}

		NodeInsert otherInsert = (NodeInsert) other;

		boolean idsEqual;
		if (m_columns == null) {
			idsEqual = (otherInsert.m_columns == null);
		} else {
			idsEqual = this.m_columns.equals(otherInsert.m_columns);
		}

		return this.m_table.equals(otherInsert.m_table) && idsEqual
				&& this.m_values.equals(otherInsert.m_values);
	}
}
