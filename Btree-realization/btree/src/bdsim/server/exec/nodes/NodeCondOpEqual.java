package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * @author wpijewsk
 * @revision $Id: NodeCondOpEqual.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class NodeCondOpEqual extends NodeCondOp {

    @Override
    public void visit(BDVisitor visitor) {
        visitor.handleCondOp(BDCondOpType.EQ);
        
    }

    @Override
    public boolean equals(Object other) {
        if(other == null || !(other instanceof NodeCondOpEqual)) {
            return false;
        } else {
            return true;
        }
    }

}
