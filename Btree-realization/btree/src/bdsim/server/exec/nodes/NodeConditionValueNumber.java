package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * A string literal in a condition.
 * 
 * @author wpijewsk
 * @revision $Id: NodeConditionValueNumber.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeConditionValueNumber extends NodeConditionValue {

	private double m_number;

	/**
	 * Class constructor
	 * 
	 * @param number
	 *            The number
	 */
	public NodeConditionValueNumber(double number) {
		this.m_number = number;
	}

	/* (non-Javadoc)
	 * @see bdsim.server.exec.nodes.Node#visit(bdsim.server.exec.BDVisitor)
	 */
	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleConditionValueNumber(this.m_number);
	}

	/* (non-Javadoc)
	 * @see bdsim.server.exec.nodes.Node#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NodeConditionValueNumber)) {
			return false;
		}
		NodeConditionValueNumber otherNumber = (NodeConditionValueNumber) other;
		return (m_number - otherNumber.m_number) < 0.00001;
	}

}
