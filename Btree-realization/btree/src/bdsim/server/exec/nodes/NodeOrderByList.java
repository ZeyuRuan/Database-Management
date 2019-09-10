package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * 
 * @author wpijewsk
 * @revision $Id: NodeOrderByList.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public class NodeOrderByList extends Node {

	private String m_column;
	private boolean m_ascending;
	private NodeOrderByList m_next;

	/**
	 * @param column
	 * @param ascending
	 * @param next
	 */
	public NodeOrderByList(String column, boolean ascending,
			NodeOrderByList next) {
		this.m_column = column;
		this.m_ascending = ascending;
		this.m_next = next;
	}

	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleOrderBy(m_column, m_ascending);

		if (m_next != null) {
			m_next.visit(visitor);
		}

	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof NodeOrderByList)) {
			return false;
		} else {
			NodeOrderByList otherList = (NodeOrderByList) other;

			boolean areColumnsEqual = m_column.equals(otherList.m_column);
			boolean areAscendingFlagsEqual = (m_ascending == otherList.m_ascending);
			boolean areNextsEqual = false;
			if (m_next == null) {
				areNextsEqual = (otherList.m_next == null);
			} else {
				areNextsEqual = m_next.equals(otherList.m_next);
			}

			return areColumnsEqual && areAscendingFlagsEqual && areNextsEqual;
		}
	}

}
