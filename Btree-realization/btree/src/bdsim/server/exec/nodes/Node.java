package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * @author wpijewsk
 * @revision $Id: Node.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public abstract class Node {

	/**
	 * Allows visitors to traverse the parse tree without seeing the details of
	 * the parse tree. Once called on the root, the callbacks in the
	 * <code>BDVisitor</code> interface will be called appropriately as the
	 * visitor traverses the tree,
	 * 
	 * @param visitor The visitor current visiting the node
	 */
    public abstract void visit(BDVisitor visitor);
    
    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public abstract boolean equals(Object other);
}
