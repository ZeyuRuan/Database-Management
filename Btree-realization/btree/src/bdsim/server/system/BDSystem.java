package bdsim.server.system;

import bdsim.server.system.concurrency.ConcurrencyController;
import bdsim.server.system.log.BDLogManager;

/**
 * @author dclee
 * @revision $Id: BDSystem.java 293 2007-01-21 16:28:18 +0000 (Sun, 21 Jan 2007) wpijewsk $
 */
public class BDSystem {
    
	public static ConcurrencyController concurrencyController;
	public static BDDiskManager diskManager;
	public static BDLogManager logManager;
	public static BDMemoryManager memoryManager;
	public static BDScheduler scheduler;
	public static BDTableManager tableManager;
	
	public static boolean isInitialized() {
		if(tableManager == null ||
				concurrencyController == null ||
				diskManager == null ||
				logManager == null ||
				memoryManager == null ||
				scheduler == null) {
			return false;
		}
		return true;
	}
}
