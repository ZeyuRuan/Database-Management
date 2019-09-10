package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * Represents a 'UPDATE' statement
 * 
 * @author wpijewsk
 * @revision $Id: NodeUpdate.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeUpdate extends NodeStatement {

	public String m_table;
	public NodeAssignmentList m_assignments;
	public NodeConditionList m_conditions;
	
	/**
	 * Class constructor.
	 * 
	 * @param table The name of the table
	 * @param assignments The assignments to perform in this UPDATE
	 * @param conditions The conditions which hold on this UPDATE
	 */
	public NodeUpdate(String table, NodeAssignmentList assignments,
			NodeConditionList conditions) {
		this.m_table = table;
		this.m_assignments = assignments;
		this.m_conditions = conditions;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see bdsim.server.exec.nodes.Node#visit(bdsim.server.exec.BDVisitor)
	 */
	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleUpdate(m_table);
		if(m_assignments != null) {
			m_assignments.visit(visitor);
		}
		if(m_conditions != null) {
			m_conditions.visit(visitor);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see bdsim.server.exec.nodes.Node#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {

		if (!(other instanceof NodeUpdate)) {
			return false;
		}

		NodeUpdate otherUpdate = (NodeUpdate) other;

		boolean condsEqual;
		if (this.m_conditions == null) {
			condsEqual = (otherUpdate.m_conditions == null);
		} else {
			condsEqual = this.m_conditions.equals(otherUpdate.m_conditions);
		}

		return this.m_table.equals(otherUpdate.m_table)
				&& this.m_assignments.equals(otherUpdate.m_assignments)
				&& condsEqual;
	}
}
