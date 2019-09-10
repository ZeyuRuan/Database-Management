package bdsim.server.main;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.log4j.PropertyConfigurator;

import bdsim.server.exec.BDPlanTest;
import bdsim.server.exec.SqlParserTest;
import bdsim.server.exec.SqlTokenTest;
import bdsim.server.exec.SqlTokenizerTest;
import bdsim.server.exec.nodes.NodeTest;
import bdsim.server.network.BDServer;
import bdsim.server.system.BDSystem;
import bdsim.server.system.concurrency.TrackableReadWriteLockTest;
//import bdsim.server.system.concurrency.WaitsForGraphTest;
import bdsim.server.system.handler.IntersectTest;
import bdsim.server.system.handler.SelectTest;
import bdsim.server.system.handler.UnionTest;

public class AllTests {

	public final static String DB_CONF = "conf/server.conf";
	public final static String LOGGER_CONF = "conf/logger.test.conf";

	public static Test suite() {
		// Initialize logger
		PropertyConfigurator.configure(LOGGER_CONF);

		// Run tests
		TestSuite suite = new TestSuite();

		// Parsing test
		suite.addTestSuite(SqlTokenTest.class);
		suite.addTestSuite(SqlTokenizerTest.class);
		suite.addTestSuite(NodeTest.class);
		suite.addTestSuite(SqlParserTest.class);

		// Plan test
		suite.addTestSuite(BDPlanTest.class);
		
		// Handler tests
		suite.addTestSuite(UnionTest.class);
		suite.addTestSuite(IntersectTest.class);
		suite.addTestSuite(SelectTest.class);

		// Concurrency tests (may crash)
		//suite.addTestSuite(WaitsForGraphTest.class);
		suite.addTestSuite(TrackableReadWriteLockTest.class);

		TestSetup wrapper = new TestSetup(suite) {
			protected void setUp() {
				new Thread() {
					public void run() {
						AllTests.oneTimeSetUp();
					}
				}.start();
			}

			protected void tearDown() {
				oneTimeTearDown();
			}
		};

		return wrapper;
	}

	public static void oneTimeSetUp() {
		try {
			BDSimulator.initSystemResources("test/files/Bank.xml");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.err
					.println("ClassNotFoundException thrown while booting...");
			System.exit(-1);
		}

		Thread schedThread = new Thread(BDSystem.scheduler, "sched");
		schedThread.start();

		// Initialize and start server
		Thread netThread = new Thread("net") {
			public void run() {
				new BDServer().startDbServer();
			}
		};
		netThread.start();

		// Wait for other threads
		try {
			schedThread.join();
			netThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void oneTimeTearDown() {
		// TODO wpijewsk Bring down server here
	}
}
