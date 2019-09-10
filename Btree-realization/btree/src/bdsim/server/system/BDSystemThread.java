package bdsim.server.system;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import bdsim.server.exec.BDPlan;
import bdsim.server.exec.BDQueryType;
import bdsim.server.system.concurrency.BDTransaction;
import bdsim.server.system.concurrency.RollbackException;
import bdsim.server.system.handler.BDDeleteHandler;
import bdsim.server.system.handler.BDHandler;
import bdsim.server.system.handler.BDInsertHandler;
import bdsim.server.system.handler.BDSelectHandler;
import bdsim.server.system.handler.BDUpdateHandler;

/**
 * A thread which executes all the plans in a given <code>BDTransaction</code>.
 * 
 * @author dclee, wpijewsk
 */
public class BDSystemThread extends Thread {

	static Logger logger = Logger.getLogger(BDSystemThread.class);
	protected int m_id;
	private BDSystemResultSet m_result;
	private BDThreadStatus m_status;
	private List<BDTable> m_tables;
	private BDTransaction m_transaction;

	/**
	 * Class constructor.
	 * 
	 * @param transaction
	 *            The transaction to execute
	 */
	public BDSystemThread(BDTransaction transaction) {
		m_transaction = transaction;
		m_status = BDThreadStatus.STARTING;
		m_result = null;
		m_id = transaction.getId();
		m_tables = new LinkedList<BDTable>();
	}
	
	public static BDSystemThread current() {
		return (BDSystemThread)Thread.currentThread();
	}
	
	/**
	 * Fake constructor for testing.
	 * 
	 * @param TID
	 *            The fake transaction ID to use
	 */
	public BDSystemThread(int id) {
		m_id = id;
	}

	public BDSystemResultSet getResult() {
		return m_result;
	}

	/**
	 * @return The status of this system thread
	 */
	public BDThreadStatus getThreadStatus() {
		return m_status;
	}

	public int getTransactionId() {
		return m_id;
	}

	public void run() {

		setThreadStatus(BDThreadStatus.RUNNING);
		
		// Add every table touched by every transaction
		for (BDPlan plan : m_transaction.getPlans()) {
			for (String tableName : plan.getTables()) {
				m_tables.add(BDSystem.tableManager.getTableByName(tableName));
			}
		}
		
		boolean done = false;
		int attempts = 1;
		
		while (!done) {			
			try {
				// Lock all tables
				for (BDTable table : m_tables) {
					boolean willModify = false;
					for (BDPlan plan : m_transaction.getPlans()) {
						if (plan.getQTtype() == BDQueryType.DELETE
								|| plan.getQTtype() == BDQueryType.INSERT
								|| plan.getQTtype() == BDQueryType.UPDATE) {
							willModify = true;
						}
					}
					if (willModify) {
						BDSystem.concurrencyController
								.lockTableForWriting(table.getName());
					} else {
						BDSystem.concurrencyController
								.lockTableForReading(table.getName());
					}
				}
								
				assert (m_transaction.getPlans().size() == 1);
				BDPlan plan = m_transaction.getPlans().getFirst();

				BDHandler handler;
				
				switch (plan.getQTtype()) {
				case INSERT:
					handler = new BDInsertHandler(plan);
					m_result = handler.execute();
					break;
				case UPDATE:
					handler = new BDUpdateHandler(plan);
					m_result = handler.execute();
					break;
				case SELECT:
					handler = new BDSelectHandler(plan);
					m_result = handler.execute();
					break;
				case DELETE:
					handler = new BDDeleteHandler(plan);
					m_result = handler.execute();
					break;
				default:
					Logger.getLogger(BDSystemThread.class).error(
							"Query type not recognized" + plan.getQTtype());
					System.err.println("Fatal error. See log.");
					System.exit(-1);
					break;
				}
				
				synchronized (BDSystemThread.class) {
					if (plan.getQTtype() == BDQueryType.DELETE
							|| plan.getQTtype() == BDQueryType.INSERT
							|| plan.getQTtype() == BDQueryType.UPDATE) {
						for (BDTuple tuple : m_result.getTupleData()) {
							if (!BDSystem.concurrencyController
									.writeDataItem(tuple)) {
								// Need to ignore operation
								for(BDTable table : m_tables) {
									table.abandon(tuple);
								}
							}
						}
					}
					
					for (BDTable table : m_tables) {
						table.commit(m_id);
					}
				}

				done = true;
				
			} catch (InterruptedException e) {
				logger.error("Interrupted while processing a query: " + e.getMessage());
				e.printStackTrace();
				System.exit(-1);
			} catch (RollbackException e) {
				logger.debug("Transaction " + m_id + " rolled back for the " + 
						attempts++ + "th time");
				rollback();
				setThreadStatus(BDThreadStatus.RUNNING);
				e.printStackTrace();
			}
		}

		// Unlock all tables
		for (BDTable table : m_tables) {
			BDSystem.concurrencyController.unlockTable(table.getName());
		}

		logger.debug("Result: " + m_result);
		
		setThreadStatus(BDThreadStatus.COMPLETE);
	}

	/**
	 * Sets this thread's status
	 * 
	 * @param ts
	 *            The new status of this thread
	 */
	public void setThreadStatus(BDThreadStatus ts) {
		m_status = ts;
		System.err.println("Thread " + getTransactionId() + " status set to " + ts);
	}
	
	public String toString() {
		return "BDSystemThread[TID=" + getTransactionId() + "]";
	}

	/**
	 * Causes this system thread to roll back its current transaction
	 * 
	 * @return The BDTransaction that was rolled back
	 */
	private void rollback() {
		for (BDTable table : m_tables) {
			table.rollback(m_id);
			table.getTableLock().abandon();
		}
	}

}
