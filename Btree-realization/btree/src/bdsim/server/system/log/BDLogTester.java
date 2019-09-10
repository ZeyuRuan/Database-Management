package bdsim.server.system.log;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;

import org.apache.log4j.Logger;

import bdsim.server.system.BDObjectType;
import bdsim.server.system.BDSchema;
import bdsim.server.system.BDTuple;

/**
 * Generates workload for the logging system, exercises that functionality, then
 * crashes the system and checks to make sure integrity constraints are held
 * when the system recovers.
 * 
 * @author wpijewsk
 */
public final class BDLogTester {
   public static BDLogTester CURRENT;
	
	/**
	 * The number of tuples used in this test
	 */
	public static final int NUM_TUPLES = 5000;
	
	/**
	 * The number of distinct operation per transaction
	 */
	private static final int NUM_OPS_PER_TRANS = 25;
	
	/**
	 * The total number of transactions
	 */
	private static final int NUM_TRANS = 100;
	
	/**
	 * The initial amount of the value in each tuple
	 */
	public static final int INIT_AMOUNT = 1000;
	
	/**
	 * The number of threads in the test (an upper bound on how many
	 * transactions can execute concurrently)
	 */
	private static final int NUM_THREADS = 20;
	
	public enum BDLogOperationType {
		ADD, EQUAL, SUBTRACT
	}

	static final String keyFieldName = "key";
	static final String valueFieldName = "value";
	
	static Logger logger = Logger.getLogger(BDLogTester.class);

	public static void main(String[] args) {
      BDLogTester.CURRENT = new BDLogTester(new BDStudentLogManager());
      BDLogTester.CURRENT.startThreads();
	}

	/**
	 * The threads which are currently executing.
	 */
	private ConcurrentLinkedQueue<BDLogThread> m_activeThreads;

	/**
	 * The schema used in the tuples for this test.
	 */
	private BDSchema m_schema;

	/**
	 * The map which stores tuples.
	 */
	private Map<Integer, BDTuple> m_tuples;

	/**
	 * The transactions which are waiting to run.
	 */
	private Queue<BDLogTransaction> m_pendingTrans;

	/**
	 * The transactions which are currently running.
	 */
	private Queue<BDLogTransaction> m_activeTrans;

	/**
	 * The next ID to assign to a running thread.
	 */
	private int m_nextId;
	
	private boolean m_crashing;

	/**
	 * The specific log manager to use.
	 */
	private BDLogManager m_logManager;

	/**
	 * Class constructor.
	 * 
	 * @param mananger
	 *            The specific log mananger to use
	 */
	private BDLogTester(BDLogManager manager) {
		final Vector<String> names = new Vector<String>();
		final Vector<BDObjectType> types = new Vector<BDObjectType>();
		names.add(keyFieldName);
		names.add(valueFieldName);
		
		types.add(BDObjectType.INTEGER);
		types.add(BDObjectType.INTEGER);
		
		m_schema = new BDSchema(names, types);
		m_tuples = new HashMap<Integer, BDTuple>();
		m_activeThreads = new ConcurrentLinkedQueue<BDLogThread>();
		m_pendingTrans = new ConcurrentLinkedQueue<BDLogTransaction>();
		m_activeTrans = new ConcurrentLinkedQueue<BDLogTransaction>();
		m_nextId = 0;
		m_logManager = manager;
		m_crashing = false;
		
		/* Generate NUM_TUPLES tuples with initial amounts of INIT_AMOUNT */
		for(int i = 0; i < NUM_TUPLES; i++) {
			BDTuple tuple = new BDTuple(m_schema);
			tuple.setObject(keyFieldName, new Integer(i));
			tuple.setObject(valueFieldName, new Integer(INIT_AMOUNT));
			m_tuples.put(i, tuple);
		}
		
		/* Generate NUM_TRANS transactions with NUM_OPS_PER_TRANS operations within them */
		for(int i = 0; i < NUM_TRANS; i++ ) {
			List<BDLogOperation> operations = new LinkedList<BDLogOperation>();
			int offset = 0;
			for (int j = 0; j < NUM_OPS_PER_TRANS - 1; j++) {
				int randVal = (int) (Math.random() * (INIT_AMOUNT / 5));
				BDLogOperationType type;
				
				if (Math.random() > 0.5) {
					type = BDLogOperationType.ADD;
					offset += randVal;
				} else {
					type = BDLogOperationType.SUBTRACT;
					offset -= randVal;
				}

				operations.add(new BDLogOperation(m_tuples.get((int) (Math
						.random() * NUM_TUPLES)), randVal, type));
			}

			/* Add one last operation to balance out all others */
			operations.add(new BDLogOperation(m_tuples.get((int) (Math
					.random() * NUM_TUPLES)), offset, BDLogOperationType.SUBTRACT));
			
			/* Save newly created transaction */
			m_pendingTrans.add(new BDLogTransaction(operations));
		}
		
		/* Kick off process which periodically "crashes" all threads */
		Thread crasher = new Thread() {
			public void run() {
				while (!m_pendingTrans.isEmpty()) {
					try {
                  Thread.sleep((long) (Math.random() * 2500) + 2500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					synchronized (this) {
						m_crashing = true;
						CyclicBarrier barrier = new CyclicBarrier(
								NUM_THREADS + 1);

						System.out
								.println("[Crasher] About to crash all threads");
						for (BDLogThread runningThread : m_activeThreads) {
							runningThread.setBarrier(barrier);
							runningThread.crashThread();
						}
						System.out.println("[Crasher] All threads crashed");

						/* Wait on for all threads to "crash" */
						while (barrier.getNumberWaiting() != m_activeThreads
								.size()) {
							System.out.println("[Crasher] Have "
									+ barrier.getNumberWaiting()
									+ " in barrier, waiting for "
									+ m_activeThreads.size() + " total, "
									+ barrier.getParties()
									+ " required to trip");
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}

						System.out
								.println("[Crasher] All ("
										+ barrier.getNumberWaiting()
										+ " in barrier, "
										+ m_activeThreads.size()
										+ " total) threads are here");

						System.out.println("[Crasher] About to reset barrier: "
								+ barrier);
						barrier.reset();

						System.out.println("[Crasher] Reset barrier");

						m_activeTrans.clear();
						reportStats();

						m_logManager.recover();
						
						m_crashing = false;
					}
				}
			}
		};
		crasher.start();
		
		/* Checkpoint the system periodically */
		Thread checkpointer = new Thread() {
			public void run() {

				while (true) { 
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					m_logManager.checkpoint();
				}
			}
		};

		checkpointer.start();
	}

	/**
	 * Starts the processing of transactions within the logging system.
	 */
	private void startThreads() {
		while (true) {
			while (m_activeThreads.size() < NUM_THREADS
					&& !m_pendingTrans.isEmpty() && !m_crashing) {
				synchronized (this) {
					logger.debug("[Main] About to dispatch [TID "
							+ m_nextId + "]");
					BDLogTransaction newTrans = m_pendingTrans.remove();
					m_activeTrans.add(newTrans);
					BDLogThread newThread = new BDLogThread(m_nextId, this,
							newTrans, m_logManager);
					m_nextId++;
					newThread.start();
					m_activeThreads.add(newThread);
				}
			}
		}
	}
	
	/**
	 * Called by a thread when it is done processing and will not change any
	 * more data.
	 * 
	 * @param trans
	 *            The <code>BDLogTransaction</code> that has just completed.
	 */
	void threadEnding(BDLogTransaction trans) {
		BDLogThread curThread = (BDLogThread) Thread.currentThread();

		m_activeThreads.remove(curThread);
		m_activeTrans.remove(trans);

		reportStats();
	}
	
	/**
	 * Called by a thread when it has crashed.
	 * 
	 * @param trans
	 *            The <code>BDLogTransaction</code> that has just crashed.
	 */
	void threadCrashing(BDLogTransaction trans) {
		BDLogThread curThread = (BDLogThread) Thread.currentThread();
		m_activeThreads.remove(curThread);
		m_activeTrans.remove(trans);
		m_pendingTrans.add(trans);
		reportStats();
	}

	/**
	 * Calculates and prints out the sums of all the tuples. May not be
	 * completely accurate if any threads are running.
	 * 
	 * @return The sum of all the tuples' values
	 */
	public int reportStats() {
		int sum = 0;
		for(BDTuple tuple: m_tuples.values()) {
			sum += (Integer) tuple.getObject(valueFieldName);
		}	

		String str = "[Stats] The sum of all the values is " + sum + "\n";
		str += "[Stats] There are " + m_pendingTrans.size()
				+ " pending transactions" + "\n";
		str += "[Stats] There are " + m_activeThreads.size()
				+ " active threads";

		if (m_activeThreads.size() > 0) {
			str += ": {";
			for (BDLogThread thread : m_activeThreads) {
				str += thread.getTransactionId() + ", ";
			}
			str = str.substring(0, str.length() - 2);
			str += "}";
		}

		System.out.println(str);
      //if (sum != (INIT_AMOUNT * NUM_TUPLES)) System.exit(-1);
		return sum;
	}
}
