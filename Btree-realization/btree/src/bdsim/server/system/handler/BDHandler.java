package bdsim.server.system.handler;

import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.concurrency.RollbackException;

/**
 * A common interface for all handler routines.
 * 
 * @author dclee
 */
public interface BDHandler {

	public BDSystemResultSet execute() throws InterruptedException, RollbackException;
}
