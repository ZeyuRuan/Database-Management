/**
 * 
 */
package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * @author wpijewsk
 * @revision $Id: NodeCondOpLike.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 *
 */
public final class NodeCondOpLike extends NodeCondOp {

    /* (non-Javadoc)
     * @see cs127db.server.sql.nodes.Node#visit(cs127db.server.sql.Visitor)
     */
    @Override
    public void visit(BDVisitor visitor) {
    	visitor.handleCondOp(BDCondOpType.LIKE);
    }

    /* (non-Javadoc)
     * @see cs127db.server.sql.nodes.Node#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object other) {
        if(other == null || !(other instanceof NodeCondOpLike)) {
            return false;
        } else {
            return true;
        }
    }

}
