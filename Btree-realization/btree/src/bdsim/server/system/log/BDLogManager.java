package bdsim.server.system.log;

import bdsim.server.system.BDTuple;

/**
 * A log that's written to disk to protect the contents of the database. Uses an
 * undo journaling and checkpointing mechanism to ensure consistency of the
 * database.
 * 
 * @author wpijewsk
 */
public interface BDLogManager {
	
	/**
	 * Tells the log manager that a data item is about to be written for this
	 * value.
	 * 
	 * @param tuple
	 *            The tuple in which the datum will be written
	 * @param column
	 *            The column of that datum
	 * @param oldObj
	 *            The old value of that datum
	 * @param newObj
	 *            The new value of that datum
	 */
	public void dataItemWritten(BDTuple tuple, String column, Object oldObj,
			Object newObj);
	
	/**
	  * Checkpoints the system, flushing changes to disk.
	 */
	public void checkpoint();
	
	/**
	 * Starts a transaction. Called by the thread which is currently executing
	 * this transaction. To get the ID of this thread, cast the current thread
	 * to a <code>BDSystemThread</code> and get the transaction ID:<br>
	 * <br>
	 * <code>((BDLogThread)Thread.currentThread()).getTransactionId()</code>
	 */
	public void startTransaction();
	
	/**
	 * Commits a transaction. Called by the thread which is currently executing
	 * this transaction. To get the ID of this thread, cast the current thread
	 * to a <code>BDSystemThread</code> and get the transaction ID:<br>
	 * <br>
	 * <code>((BDLogThread)Thread.currentThread()).getTransactionId()</code>
	 */
	public void commitTransaction();
	
	/**
	 * Recovers from a failure. Reads the log and performs the appropriate
	 * actions. These actions involve (since the last checkpoint) undoing
	 * transactions which did not commit and redoing transactions which did
	 * commit.  Check the book (Section 17.5.4) for exact details. 
	 */
	public void recover();
}
