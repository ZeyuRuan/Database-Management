package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * Represent a string literal.
 * 
 * @author wpijewsk
 * @revision $Id: NodeValueNumber.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeValueNumber extends NodeValue {

    private double m_number;

    /**
     * Class constructor
     * 
     * @param number
     *            The number
     */
    public NodeValueNumber(double number) {
        this.m_number = number;
    }

    /*
     * (non-Javadoc)
     * 
     * @see cs127db.server.sql.nodes.Node#visit(cs127db.server.sql.Visitor)
     */
    @Override
    public void visit(BDVisitor visitor) {
    	visitor.handleNumber(m_number);
    }

    /*
     * (non-Javadoc)
     * 
     * @see cs127db.server.sql.nodes.Node#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof NodeValueNumber)) {
            return false;
        } else {
            NodeValueNumber otherNumber = (NodeValueNumber) other;
            return m_number == otherNumber.m_number;
        }
    }

	public Double getNumber() {
		return m_number;
	}

}
