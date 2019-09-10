package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * 
 * @author wpijewsk
 * @revision $Id: NodeColumnStar.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 *
 */
public class NodeColumnStar extends NodeColumnClause {

	@Override
	public void visit(BDVisitor visitor) {
		visitor.handleColumnDeclStar();
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof NodeColumnStar)) {
			return false;
		} else {
			return true;
		}
	}

}
