package bdsim.server.system.concurrency;

import bdsim.server.system.BDTuple;

/**
 * A separate thread that watches for synchronization problems with the
 * currently executing transactions. Provides methods for the handlers to tell
 * the system that the handler is accessing or writing to a particular data
 * item. This interface aims to abstract over the differences between the two
 * lock mangaers to provide a consistent view of synchronization to all the
 * handlers.
 * 
 * @author wpijewsk, acath
 */
public abstract class ConcurrencyController extends Thread {
	public abstract void run();
	
	/**
	 * Acquires a lock for a specific table for reading.
	 * 
	 * @param table
	 *            The table on which to acquire the lock 
	 * @return The status of the lock
	 * @throws RollbackException 
	 */
	public abstract void lockTableForReading(String table) throws RollbackException;
	
	/**
	 * Acquires a lock for a specific table for writing.
	 * 
	 * @param table
	 *            The table on which to acquire the lock 
	 * @return The status of the lock
	 * @throws RollbackException 
	 */
	public abstract void lockTableForWriting(String table) throws RollbackException;
	
	/**
	 * Unlocks a table.
	 * 
	 * @param table
	 *            The table to unlock
	 * @return The status of the lock
	 */
	public abstract void unlockTable(String table);
	
	/**
	 * Signals the concurrency controller that a handler is about to read a data
	 * item. Note: this method needs to be called in a synchronized block, and
	 * in the same synchronized block, the read of the data item should happen.
	 * This property is not enforced, so please use caution when using this
	 * method. Think about what you should be synchronizing on.
	 * 
	 * @param tuple
	 *            The tuple that's about to be read
	 * @throws RollbackException
	 *             If the transaction should be rolled back because transaction
	 *             ordering was violated
	 */
	public abstract void readDataItem(BDTuple tuple) throws RollbackException;

	/**
	 * Signals the concurrency controller that a handler is about to write a
	 * data item. Note: this method needs to be called in a synchronized block,
	 * and in the same synchronized block, the write of the data item should
	 * happen. This property is not enforced, so please use caution when using
	 * this method. Think about what you should be synchronizing on.
	 * 
	 * @param tuple
	 *            The tuple that's about to be written
	 * @return True if the item was written, false if it was ignored because it
	 *         was obselete
	 * @throws RollbackException
	 *             If the transaction should be rolled back because transaction
	 *             ordering was violated
	 */
	public abstract boolean writeDataItem(BDTuple tuple) throws RollbackException;
}
