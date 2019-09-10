package bdsim.server.system.concurrency;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemThread;
import bdsim.server.system.BDTable;
import bdsim.server.system.BDTuple;

/**
 * Breaks deadlocks using the strict two-phase locking algorithm. Generates a
 * graph of dependencies between transactions, and then checks for cycles in
 * that graph.
 * 
 * @author dclee, wpijewsk
 */
public class TwoPhaseLockController extends ConcurrencyController {

	static Logger logger = Logger.getLogger(TwoPhaseLockController.class);
	private final int DELAY;
	
	public TwoPhaseLockController() {
		DELAY = Integer.parseInt(System.getProperty("controller.delay"));
	}
			
	/**
	 * Breaks any detected cycles by aborting the transaction which has been
	 * running for the least amount of time.
	 */
	public void run() {
		while (true) {
			logger.debug("Running 2PL concurrency checker...");
			
			synchronized (BDSystem.class) {
			
				Set<BDTrackableReadWriteLock<BDSystemThread>> locks =
					new HashSet<BDTrackableReadWriteLock<BDSystemThread>>();
				
				for (BDTable table : BDSystem.tableManager.getTables().values()) {
					locks.add(table.getTableLock());
				}				
				
				WaitsForGraph graph = new WaitsForGraph(locks);
				
				if (graph.hasCycle()) {
					int tid = graph.getCycleBreaker();
					logger.debug("    Cycle found, rolling back transaction " + tid);
					BDSystem.scheduler.rollback(tid);
				}
			}
			logger.debug("...done");
			 
			try {
				Thread.sleep(DELAY);
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			}
		}
	}

	public void lockTableForReading(String table) throws RollbackException {
      BDSystem.tableManager.getTableByName(table).getTableLock().readLock();
	}

	public void lockTableForWriting(String table) throws RollbackException {
      BDSystem.tableManager.getTableByName(table).getTableLock().writeLock();
	}

	public void unlockTable(String table) {
      BDSystem.tableManager.getTableByName(table).getTableLock().unlock();
	}

	/*
	 * Note: it is not necessary to implement these methods, as they only apply
	 * to the TimeStampController.
	 */	
	public void readDataItem(BDTuple tuple) throws RollbackException {}
	public boolean writeDataItem(BDTuple tuple) throws RollbackException {
		return true;
	}
}
