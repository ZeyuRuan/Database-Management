package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * A string literal in a condition.
 * 
 * @author wpijewsk
 * @revision $Id: NodeConditionValueLiteral.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeConditionValueLiteral extends NodeConditionValue {

	private String m_literal;

	/**
	 * Class constructor.
	 * 
	 * @param literal
	 */
	public NodeConditionValueLiteral(String literal) {
		// TODO Auto-generated constructor stub
		this.m_literal = literal;
	}

	/* (non-Javadoc)
	 * @see bdsim.server.exec.nodes.Node#visit(bdsim.server.exec.BDVisitor)
	 */
	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleConditionValueLiteral(this.m_literal);

	}

	/* (non-Javadoc)
	 * @see bdsim.server.exec.nodes.Node#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NodeConditionValueLiteral)) {
			return false;
		}
		NodeConditionValueLiteral otherLiteral = (NodeConditionValueLiteral) other;
		return m_literal.equals(otherLiteral.m_literal);
	}

}
