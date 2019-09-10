package bdsim.server.system.concurrency;

import bdsim.server.system.BDSystemThread;
import bdsim.server.system.BDTuple;

/**
 * Uses a timestamp ordering protocol to serialize concurrent operations. Some
 * of these methods are empty since they only apply to lock-based protocols.<br>
 * <br>
 * Look in the chapter on Concurrency Control in your book for more detailed
 * information about how these validations work.
 * 
 * @author wpijewsk
 */
public class TimeStampController extends ConcurrencyController {

	/**
	 * This method can be empty since the timestamp protocol doesn't need a
	 * particular thread to run.
	 */
	public void run() {}
	
	/*
	 * Note: it is not necessary to implement these methods, as they only apply
	 * to the TwoPhaseLockController.
	 */	
	public void lockTableForReading(String table) {}
	public void lockTableForWriting(String table) {}
	public void unlockTable(String table) {}

	/**
	 * Checks to make sure that this read operation is occuring in the proper
	 * order compared to the ordering of the transcations entering the system.
	 * <br>
	 * Implements the following logic: <br>
	 * 1. If TID < W-timestamp, then the read operation is rejected since it is
	 * reading a stale value. <br>
	 * 2. If TID >= W-timestamp, then the read operation succeeds and updates
	 * the read timestamp. <br>
	 * <br>
	 * Note: You'll want to look at the comment in the ConcurrencyController
	 * class for more information about the context in which this function
	 * should run.
	 * 
	 * @param tuple
	 *            The tuple being read
	 * @param TID
	 *            The ID of the transaction reading the tuple
	 */
	public void readDataItem(BDTuple tuple) throws RollbackException {
		synchronized (tuple) {
			int tid = ((BDSystemThread)Thread.currentThread()).getTransactionId();
			int wtid = tuple.getWriteTimestamp();
			int rtid = tuple.getReadTimestamp();
			//
			// Check whether the current timestamp is before the wtid
			//
			if (tid < wtid) {
				System.err.println("Transaction tid #" + tid + " is before tuple " + tuple + " write tid #" + wtid);
				throw new RollbackException();
			}
			tuple.setReadTimestamp(Math.max(rtid, tid));
		} // SYNCHRONIZED
	}

	/**
	 * Checks to make sure that this write operation is occuring in the proper
	 * order compared to the ordering of the transcations entering the system.
	 * <br>
	 * Implements the following logic: <br>
	 * 1. If TID < R-timestamp, then that read operation missed this write
	 * operation, and the write operation is rolled back. <br>
	 * 2. If TID < W-timestamp, the write is ignored. (Thomas' Write Rule) <br>
	 * 3. Otherwise, the system executes the write operation and updates the
	 * write timestamp.<br>
	 * <br>
	 * Note: You'll want to look at the comment in the ConcurrencyController
	 * class for more information about the context in which this function
	 * should run.
	 * 
	 * @param tuple
	 *            The tuple being read
	 * @param TID
	 *            The ID of the transaction reading the tuple
	 * @return Whether the write is valid
	 */
	public boolean writeDataItem(BDTuple tuple) throws RollbackException {
		synchronized (tuple) {
			int tid = ((BDSystemThread)Thread.currentThread()).getTransactionId();
			int wtid = tuple.getWriteTimestamp();
			int rtid = tuple.getReadTimestamp();
			//
			// Check whether the current timestamp is before the wtid
			//
			if (tid < rtid || tid < wtid) {
				System.err.println("Transaction tid #" + tid + " is before tuple " + tuple + " read/write tid #" + rtid + "/" + wtid);
				throw new RollbackException();
			}
			tuple.setWriteTimestamp(Math.max(rtid, tid));
		} // SYNCHRONIZED
		return true;
	
	}
}
