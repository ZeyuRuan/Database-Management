package bdsim.common;

import java.util.List;

import bdsim.server.system.BDSchema;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * Interface to standardize ways to access a result set. Used by the client to
 * restrict access to a <code>BDSystemResultSet</code>.
 * 
 * @author wpijewsk
 * @revision $Id: BDResultSet.java 274 2007-01-20 02:56:25 +0000 (Sat, 20 Jan 2007) wpijewsk $
 */
public interface BDResultSet {
	
    /**
     * @return  The data in this result set
     */
    public List<? extends BDRow> getData();
    
    /**
     * @return The column names of the result set
     */
    public BDSchema getSchema();
    
    /**
     * Adds a row to this result set
     * 
     * @param tuple The row to add
     * @throws RollbackException
     */
    public void addRow(BDTuple tuple) throws RollbackException;
}
