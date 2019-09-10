package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * @author wpijewsk
 * @revision $Id: NodeConditionList.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public class NodeConditionList extends Node {
    
    private NodeCondition m_condition;
    private NodeBoolOp m_boolop;
    private NodeConditionList m_next;

    /**
     * Class constructor.
     * 
     * @param condition
     * @param boolop
     * @param next
     */
    public NodeConditionList(NodeCondition condition, NodeBoolOp boolop,
            NodeConditionList next) {
        this.m_condition = condition;
        this.m_boolop = boolop;
        this.m_next = next;
    }

    /**
     * @see bdsim.server.exec.nodes.Node#visit(bdsim.server.exec.BDVisitor)
     */
    public void visit(BDVisitor visitor) {
    	m_condition.visit(visitor);
    	
    	if(m_boolop != null) {
    		m_boolop.visit(visitor);
    	}
    	
    	if(m_next != null) {
    		m_next.visit(visitor);
    	}

    }

    /* (non-Javadoc)
     * @see cs127db.server.sql.nodes.Node#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object other) {
        if ((other == null) || !(other instanceof NodeConditionList)) {
            return false;
        }
        NodeConditionList otherList = (NodeConditionList) other;
        
        boolean areConditionsEqual = m_condition.equals(otherList.m_condition);
        
        boolean areBoolOpsEqual = false;
        if(m_boolop == null) {
            areBoolOpsEqual = (otherList.m_boolop == null);
        } else {
            areBoolOpsEqual = m_boolop.equals(otherList.m_boolop);
        }
        
        boolean areNextsEqual = false;
        if(m_next == null) {
            areNextsEqual = (otherList.m_next == null);
        } else {
            areNextsEqual = m_next.equals(otherList.m_next);
        }

        return areBoolOpsEqual && areConditionsEqual && areNextsEqual;
    }
    
    public NodeCondition getCondition() {
    	return m_condition;
    }
    
    public NodeBoolOp getBoolOp() {
    	return m_boolop;
    }
    
    public NodeConditionList getRestList() {
    	return m_next;
    }

}
