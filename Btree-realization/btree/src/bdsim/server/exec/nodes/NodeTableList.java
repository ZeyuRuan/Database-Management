package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * A list of tables. 
 * @author wpijewsk
 * @revision $Id: NodeTableList.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeTableList extends Node {

	private String m_table;
	private String m_identifier;
	private BDJoinType m_jointype;
	private NodeTableList m_next;

	/**
	 * Class Constructor.
	 * 
	 * @param table
	 * @param identifier
	 * @param joinType
	 * @param next
	 */
	public NodeTableList(String table, String identifier, BDJoinType joinType,
			NodeTableList next) {
		this.m_table = table;
		this.m_identifier = identifier;
		this.m_jointype = joinType;
		this.m_next = next;
	}

	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleTable(m_table, m_identifier, m_jointype);
		
		if (m_next != null) {
			m_next.visit(visitor);
		}

	}

	@Override
	public boolean equals(Object other) {
		if ((other == null) || !(other instanceof NodeTableList)) {
			return false;
		}
		NodeTableList otherList = (NodeTableList) other;
		boolean areNamesEqual = m_table.equals(otherList.m_table);
		boolean areIdsEqual = false;
		if (m_identifier == null) {
			areIdsEqual = (otherList.m_identifier == null);
		} else {
			areIdsEqual = m_identifier.equals(otherList.m_identifier);
		}
		
		boolean areJoinTypesEqual = m_jointype.equals(otherList.m_jointype);

		boolean areNextsEqual = false;
		if (m_next == null) {
			areNextsEqual = (otherList.m_next == null);
		} else {
			areNextsEqual = m_next.equals(otherList.m_next);
		}

		return areNamesEqual && areIdsEqual && areJoinTypesEqual
				&& areNextsEqual;
	}
}