package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * @author wpijewsk
 * @revision $Id: NodeValueList.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeValueList extends Node {

	private NodeValueList m_rest;
	private NodeValue m_value;
	
	/**
	 * Class constructor.
	 * 
	 * @param value
	 * @param rest
	 */
	public NodeValueList(NodeValue value, NodeValueList rest) {
		this.m_value = value;
		this.m_rest = rest;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NodeValueList)) {
			return false;
		}

		NodeValueList otherValueList = (NodeValueList) other;
		
		boolean restEqual;
		if (m_rest == null) {
			restEqual = (otherValueList.m_rest == null);
		} else {
			restEqual = this.m_rest.equals(otherValueList.m_rest);
		}

		return this.m_value.equals(otherValueList.m_value) && restEqual;
	}

	/* (non-Javadoc)
	 * @see bdsim.server.exec.nodes.Node#visit(bdsim.server.exec.BDVisitor)
	 */
	@Override
	public void visit(BDVisitor visitor) {
		m_value.visit(visitor);
		if(m_rest != null) {
			m_rest.visit(visitor);
		}
	}

}
