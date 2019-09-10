package bdsim.server.main;

import java.io.IOException;

import junit.framework.TestCase;
import bdsim.client.BDSimpleClient;
import bdsim.common.BDResponseAdapter;
import bdsim.common.messages.BDErrorResponse;
import bdsim.common.messages.BDResultResponse;

/**
 * TODO add some tests that cause rollback
 * 
 * 
 * @author acath
 */
public class StressTest extends TestCase {

	private static final int port = 1877;
	private static final String host = "localhost";
	private static final String[] queries = {"SELECT * FROM Accounts",
		 "SELECT * FROM Customers", 
		 "INSERT into Customers VALUES (21., 'Adam', 'Conrad', '012-34-5678')", 
		 "INSERT into Accounts VALUES (21., 29.0, 'Savings', 10000.00)"};

	private class NonSuckyThread extends Thread {

		public int m_arg;

		public NonSuckyThread(int arg) {
			m_arg = arg;
		}

	}

	private int m_numClients = 4;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

	}

	public void testConcurrently() throws IOException, ClassNotFoundException,
		InterruptedException {
	
		Thread[] threads = new Thread[m_numClients];
		
		for (int ii = 0; ii < m_numClients; ii++) {
			threads[ii] = new Thread() {
				public void run() {
					BDSimpleClient connection = new BDSimpleClient(host, 1877);
					try {
						for (int x = 0; x < 10; x++) {
							connection.request(queries[(int)(Math.random()*10 + 1)% 4],
									new BDResponseAdapter() {
										@Override
										public void handle(
												BDResultResponse response,
												long elapsed) {
										}
									});
							yield();
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					} 
				}
			};
		
			threads[ii].start();
		}
		
		for (int ii = 0; ii < m_numClients; ii++) {
			threads[ii].join();
		}
	}

	public void testDisjointInsertsAndDeletes() throws IOException,
			ClassNotFoundException, InterruptedException {

		BDSimpleClient connection = new BDSimpleClient(host, 1877);
		Thread[] threads = new Thread[m_numClients];
		final int rowsPerThread = 10;

		// create a bunch of clients, each of which creates a bunch of rows
		for (int ii = 0; ii < m_numClients; ii++) {
			threads[ii] = new NonSuckyThread(ii) {
				public void run() {
					BDSimpleClient connection = new BDSimpleClient(host, port);
					try {
						// create rowsPerThread rows in the Customers table
						for (int k = 0; k < rowsPerThread; k++) {
							yield();
							connection.request(
									"INSERT INTO Customers (id) VALUES ("
											+ ((m_arg + 1) * 100 + k) + ")",
									new BDResponseAdapter());
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			};
			threads[ii].start();
		}

		// wait for all clients to create their rows
		for (int ii = 0; ii < m_numClients; ii++) {
			threads[ii].join();
		}

		// select all the newly created rows and check them
		connection.request("SELECT id FROM Customers WHERE id >= 100",
				new BDResponseAdapter() {
					@Override
					public void handle(BDErrorResponse response, long elapsed) {
						fail(response.getMessage());
					}

					@Override
					public void handle(BDResultResponse response, long elapsed) {
						assertEquals(m_numClients * rowsPerThread,
								response.getResults().getData().size());
					}
				});

		// create a bunch more clients, each of which deletes the rows created by 
		// its predecessor
		for (int ii = 0; ii < m_numClients; ii++) {
			threads[ii] = new NonSuckyThread(ii) {
				public void run() {
					try {
						BDSimpleClient connection = new BDSimpleClient(host,
								port);
						for (int k = 0; k < rowsPerThread; k++) {
							yield();
							connection.request(
									"DELETE FROM Customers WHERE id="
											+ ((m_arg + 1) * 100 + k),
									new BDResponseAdapter());
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			};
			threads[ii].start();
		}

		// wait for all clients to delete their rows
		for (int ii = 0; ii < m_numClients; ii++) {
			threads[ii].join();
		}

		// select all rows and ensure that they're all gone
		connection.request("SELECT id FROM Customers WHERE id >= 100",
				new BDResponseAdapter() {
					@Override
					public void handle(BDErrorResponse response, long elapsed) {
						fail(response.getMessage());
					}

					@Override
					public void handle(BDResultResponse response, long elapsed) {
						//assert (response.getResults() != null);
						if (response.getResults() != null) {
							assertEquals(0, response.getResults().getData()
									.size());
						}
					}
				});
	}
}
