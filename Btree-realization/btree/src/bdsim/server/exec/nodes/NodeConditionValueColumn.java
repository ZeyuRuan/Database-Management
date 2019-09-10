package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * A column name in a condition.
 * 
 * @author wpijewsk
 * @revision $Id: NodeConditionValueColumn.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeConditionValueColumn extends NodeConditionValue {

	private String m_column;
	
	/**
	 * Class constructor.
	 * 
	 * @param column
	 */
	public NodeConditionValueColumn(String column) {
		// TODO Auto-generated constructor stub
		this.m_column = column;
	}

	/* (non-Javadoc)
	 * @see bdsim.server.exec.nodes.Node#visit(bdsim.server.exec.BDVisitor)
	 */
	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleConditionValueColumn(this.m_column);
	}

	/* (non-Javadoc)
	 * @see bdsim.server.exec.nodes.Node#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NodeConditionValueColumn)) {
			return false;
		}
		NodeConditionValueColumn otherColumn = (NodeConditionValueColumn) other;
		return m_column.equals(otherColumn.m_column);
	}

}
