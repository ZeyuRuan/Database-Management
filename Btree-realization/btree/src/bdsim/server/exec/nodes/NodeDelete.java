package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * Represents an DELETE statement.
 * 
 * @author wpijewsk
 * @revision $Id: NodeDelete.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeDelete extends NodeStatement {

	private boolean m_all;
	private String m_table;
	private NodeConditionList m_conditions;
	
	/**
	 * Class constructor.
	 * 
	 * @param all Whether or not this <code>NodeDelete</code> deletes all the
	 * rows in a table
	 * @param table The table to delete from
	 * @param conditions The conditions that hold on the rows to delete
	 */
	public NodeDelete(boolean all, String table, NodeConditionList conditions) {
		this.m_all = all;
		this.m_table = table;
		this.m_conditions = conditions;
	}

	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleDelete(m_all, m_table);
		
		if(m_conditions != null) {
			m_conditions.visit(visitor);
		}
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NodeDelete)) {
			return false;
		}
		NodeDelete otherDelete = (NodeDelete) other;

		boolean condsEqual;
		if (this.m_conditions == null) {
			condsEqual = (otherDelete.m_conditions == null);
		} else {
			condsEqual = this.m_conditions.equals(otherDelete.m_conditions);
		}
		
		return this.m_all == otherDelete.m_all
				&& this.m_table.equals(otherDelete.m_table) && condsEqual;
	}

}
