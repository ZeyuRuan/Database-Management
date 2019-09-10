package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * A condition. A condition is two condition values (can be either columns,
 * string literals, or numbers) that are compared with a comparison operator.
 * 
 * @author wpijewsk
 * @revision $Id: NodeCondition.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public class NodeCondition extends Node {

    private NodeConditionValue m_lhs;
    private NodeCondOp m_condop;
    private NodeConditionValue m_rhs;
    
    /**
	 * Class constructor.
	 * 
	 * @param lhs  The left-hand side of this condition
	 * @param condop  The condition value linking the left and right sides
	 * @param rhs  The right-hand side of this condition
	 */
	public NodeCondition(NodeConditionValue lhs, NodeCondOp condop,
			NodeConditionValue rhs) {
		this.m_lhs = lhs;
		this.m_condop = condop;
		this.m_rhs = rhs;
	}

    /*
	 * (non-Javadoc)
	 * 
	 * @see cs127db.server.sql.nodes.Node#visit(cs127db.server.sql.Visitor)
	 */
    @Override
    public void visit(BDVisitor visitor) {
    	visitor.handleCondition(this);
    	
    	m_lhs.visit(visitor);
    	m_condop.visit(visitor);
    	m_rhs.visit(visitor);
    }

    /* (non-Javadoc)
     * @see cs127db.server.sql.nodes.Node#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object other) {
        if ((other == null) || !(other instanceof NodeCondition)) {
            return false;
        }
        NodeCondition otherCondition = (NodeCondition) other;
        
        boolean areColumnsEqual = m_lhs.equals(otherCondition.m_lhs);
        
        boolean areCondOpsEqual = false;
        if(m_condop == null) {
            areCondOpsEqual = (otherCondition.m_condop == null);
        } else {
            areCondOpsEqual = m_condop.equals(otherCondition.m_condop);
        }
        
        boolean areNextsEqual = false;
        if(m_rhs == null) {
            areNextsEqual = (otherCondition.m_rhs == null);
        } else {
            areNextsEqual = m_rhs.equals(otherCondition.m_rhs);
        }

        return areCondOpsEqual && areColumnsEqual && areNextsEqual;
    }
    
}
