package bdsim.server.system;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import bdsim.server.system.concurrency.BDTransaction;
import org.apache.log4j.Logger;

/**
 * This class schedules the execution of all operations for the system internals.
 * All operations are created by passing this class a plan, and these plans
 * are subsequently given BDSystemThreads to run in.
 * 
 * @author dclee, wpijewsk, acath
 * @revision $Id: BDScheduler.java 292 2007-01-21 01:43:46 +0000 (Sun, 21 Jan 2007) wpijewsk $
 */
public class BDScheduler implements Runnable {
   
   /**
    * This hook allows us to insert a BDSystemThread into the scheduler
    * without having to wrap it in a transaction with a query plan
    * @param thread
    */
   public void addThread(BDSystemThread thread) {
      this.m_threads.add(thread);
   }
   

	static Logger logger = Logger.getLogger(BDScheduler.class);
	private Vector<BDSystemThread> m_threads;
	private Vector<BDTransaction> m_transactions;
	private int m_maxThreads;
	private int m_transactionCounter;
	private Map<Integer, Long> m_startTimes;
	
	/**
	 * Initializes the schedule with a maximum number of threads that be be run
	 * at a given time.
	 * 
	 * @param maxThreads
	 *            The maximum number of threads.
	 */
	public BDScheduler(int maxThreads) {
		m_maxThreads = maxThreads;
		m_threads = new Vector<BDSystemThread>();
		m_transactions = new Vector<BDTransaction>();
		m_transactionCounter = 0;
		m_startTimes = new HashMap<Integer,Long>();
	}
	
	/**
	 * Adds a plan to the plan queue to be executed.
	 * 
	 * @param trans
	 *            the plan to be added
	 * @return The transaction ID of the plan
	 */
	public synchronized int addTransaction(BDTransaction trans) {
		m_transactions.add(trans);
		m_transactionCounter += 1;
		trans.setId(m_transactionCounter);
		m_startTimes.put(m_transactionCounter, System.currentTimeMillis());
		return m_transactionCounter;
	}
	
	/**
	 * Gets the result for a given transaction. Should only be called if the
	 * transaction isFinished
	 * 
	 * @param TID
	 *            The transaction ID to get the result for
	 * @return the resultant thread of the transaction
	 */
	public BDSystemResultSet getResult(int TID) {
		BDSystemResultSet r = null;
		BDSystemThread removeThis = null;
		synchronized (m_threads) {
			for (BDSystemThread t : m_threads) {
				if (t.getTransactionId() == TID) {
					r = t.getResult();
					removeThis = t;
				}
			}
			m_threads.removeElement(removeThis);
		}
		return r;
	}
	
	/**
	 * Gets the starting time of a transacation.
	 * 
	 * @param tid  A transaction id
	 * @return  The starting time of that transaction
	 */
	public long getStartTime(int tid) {
		if (m_startTimes.get(tid) != null)
			return m_startTimes.get(tid);
		return 0;
	}

	/**
	 * Called by the frontend of the server in order to check if a given
	 * transaction thread has completed.
	 * 
	 * @param TID
	 *            the integer ID of the transaction to be checked
	 * @return true if the transaction's thread has completed, false if not
	 */
	public boolean isFinished(int TID) {
		synchronized (m_threads) {
			for (BDSystemThread t : m_threads) {
				if (t.getTransactionId() == TID) {
					if (t.getThreadStatus() == BDThreadStatus.COMPLETE)
						return true;
				}
			}
		}
		return false;
	}

	/**
	 * This is extremely dangerous. You should only call it on a deadlocked
	 * thread. It's already called once from the deadlock detector - you (the
	 * student) never need to call it again.
	 * 
	 * @param tid
	 *            Transaction ID to rollback
	 */
	public void rollback(int tid) {
		for (BDSystemThread thread : m_threads) {
		   System.err.println("THREAD TID: " + thread.getTransactionId());
			if (thread.getTransactionId() == tid) {
			   System.err.println("ROLLING BACK: " + tid);
				thread.setThreadStatus(BDThreadStatus.ROLLING_BACK);
				thread.interrupt();
			}
		}
	}

	/**
	 * The main loop of the scheduler. This algorithm is really basic - if any
	 * BDSystem thread wants to start, the scheduler will start that thread.
	 */
	public void run() {
		BDSystem.concurrencyController.start();

		while (true) {
			if (!m_transactions.isEmpty()) {
				if (m_threads.size() < m_maxThreads) {
					BDSystemThread t = new BDSystemThread(m_transactions
							.remove(0));
					m_threads.add(t);
					t.start();
				}
			}		
			
			try {
				Thread.sleep(5);
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
}