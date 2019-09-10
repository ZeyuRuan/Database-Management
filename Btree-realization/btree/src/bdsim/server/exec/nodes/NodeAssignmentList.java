package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * Represents a list of assignments within a 'UPDATE' statement
 * 
 * @author wpijewsk
 * @revision $Id: NodeAssignmentList.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeAssignmentList extends Node {

	public String m_column;
	public NodeValue m_value;
	public NodeAssignmentList m_rest;

	/**
	 * Class constructor.
	 * 
	 * @param column
	 * @param value
	 * @param rest
	 */
	public NodeAssignmentList(String column, NodeValue value,
			NodeAssignmentList rest) {
		this.m_column = column;
		this.m_value = value;
		this.m_rest = rest;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see bdsim.server.exec.nodes.Node#visit(bdsim.server.exec.BDVisitor)
	 */
	@Override
	public void visit(BDVisitor visitor) {
		if (m_value instanceof NodeValueLiteral) {
			NodeValueLiteral valueLiteral = (NodeValueLiteral) m_value;
			visitor.handleAssignment(m_column, valueLiteral.getLiteral());
		} else if (m_value instanceof NodeValueNumber) {
			NodeValueNumber valueNumber = (NodeValueNumber) m_value;
			visitor.handleAssignment(m_column, valueNumber.getNumber());
		}

		if (m_rest != null) {
			m_rest.visit(visitor);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {

		if (!(other instanceof NodeAssignmentList)) {
			return false;
		}

		NodeAssignmentList otherList = (NodeAssignmentList) other;

		boolean restEquals;
		if (m_rest == null) {
			restEquals = (otherList.m_rest == null);
		} else {
			restEquals = this.m_rest.equals(otherList.m_rest);
		}

		return m_column.equals(otherList.m_column)
				&& m_value.equals(otherList.m_value) && restEquals;
	}
}