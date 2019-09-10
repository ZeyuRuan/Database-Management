/**
 * 
 */
package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * @author wpijewsk
 * @revision $Id: NodeCondOpLessThanOrEqual.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeCondOpLessThanOrEqual extends NodeCondOp {

    /* (non-Javadoc)
     * @see cs127db.server.sql.nodes.Node#visit(cs127db.server.sql.Visitor)
     */
    @Override
    public void visit(BDVisitor visitor) {
    	visitor.handleCondOp(BDCondOpType.LTEQ);
    }

    /* (non-Javadoc)
     * @see cs127db.server.sql.nodes.Node#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object other) {
        if(other == null || !(other instanceof NodeCondOpLessThanOrEqual)) {
            return false;
        } else {
            return true;
        }
    }

}
