package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * Represent a string literal.
 * 
 * @author wpijewsk
 * @revision $Id: NodeValueLiteral.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeValueLiteral extends NodeValue {

    private String m_literal;

    /**
     * Class constructor. <br>
     * Note: The paramater <code>literal</code> should not contain quotes
     * 
     * @param literal
     *            The string literal
     */
    public NodeValueLiteral(String literal) {
        this.m_literal = literal;
    }

    /*
     * (non-Javadoc)
     * 
     * @see cs127db.server.sql.nodes.Node#visit(cs127db.server.sql.Visitor)
     */
    @Override
    public void visit(BDVisitor visitor) {
        visitor.handleLiteral(m_literal);
    }

    /*
     * (non-Javadoc)
     * 
     * @see cs127db.server.sql.nodes.Node#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof NodeValueLiteral)) {
            return false;
        } else {
            NodeValueLiteral otherLiteral = (NodeValueLiteral) other;
            return m_literal.equals(otherLiteral.m_literal);
        }
    }

	public String getLiteral() {
		return m_literal;
	}
}
