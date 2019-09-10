package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * @author wpijewsk
 * @revision $Id: NodeIdList.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeIdList extends Node {

	private String m_id;
	private NodeIdList m_rest;

	/**
	 * Class constructor.
	 * @param id  The id of this part of the list
	 * @param rest  The rest of the list
	 */
	public NodeIdList(String id, NodeIdList rest) {
		// TODO Auto-generated constructor stub
		this.m_id = id;
		this.m_rest = rest;
	}

	@Override
	public void visit(BDVisitor visitor) {
        visitor.handleColumnDecl(m_id, "");

		if (m_rest != null) {
			m_rest.visit(visitor);
		}
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NodeIdList)) {
			return false;
		}

		NodeIdList otherIdList = (NodeIdList) other;

		boolean restEqual;
		if (m_rest == null) {
			restEqual = (otherIdList.m_rest == null);
		} else {
			restEqual = this.m_rest.equals(otherIdList.m_rest);
		}

		return this.m_id.equals(otherIdList.m_id) && restEqual;
	}
}
