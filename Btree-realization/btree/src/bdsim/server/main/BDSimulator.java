package bdsim.server.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.io.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import bdsim.server.network.BDServer;
import bdsim.server.system.BDDiskManager;
import bdsim.server.system.BDMemoryManager;
import bdsim.server.system.BDScheduler;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDTableManager;
import bdsim.server.system.BDTableManager.XmlException;
import bdsim.server.system.concurrency.ConcurrencyController;
import bdsim.server.system.concurrency.TimeStampController;
import bdsim.server.system.concurrency.TwoPhaseLockController;
import bdsim.server.system.log.BDStudentLogManager;
import bdsim.server.system.log.BDLogManager;
import bdsim.client.BDSimpleClient;
import bdsim.server.system.index.*;


/**
 * Top-level class for the BDSim system. Initializes all of the components of
 * the database system, which include the system internals (disk, memory
 * manager, etc.), the networking, and other miscellaneous modules.
 * 
 * @author wpijewsk, dclee
 * @revision $Id: BDSimulator.java 304 2007-01-22 19:55:30 +0000 (Mon, 22 Jan 2007) wpijewsk $
 */
public final class BDSimulator {

    private final static String DB_CONF = "conf/server.conf";
    static Logger logger = Logger.getLogger(BDSimulator.class);
    private final static String LOGGER_CONF = "conf/logger.conf";
	
    //
    // Pavlo Hack
    //
    public static BDBPlusTreeVisualizer visualizer;
    
	private static ConcurrencyController createConcurrencyController() throws ClassNotFoundException {
		String className = System.getProperty("controller.className");
		if ("TimeStampController".equals(className)) {
			return new TimeStampController();
		} else if ("TwoPhaseLockController".equals(className)) {
			return new TwoPhaseLockController();			
		} else {
			throw new ClassNotFoundException("Non-existent concurrency controller: " + 
					className);
		}
	}
	
	private static BDDiskManager createDiskManager() {
		return new BDDiskManager();
	}

	private static BDLogManager createLogManager() {
		return new BDStudentLogManager();
	}

	private static BDMemoryManager createMemoryManager() {
		return new BDMemoryManager(1024, 100000);
	}

	private static BDScheduler createScheduler() {
		int numSysThreads = Integer.parseInt(System
				.getProperty("scheduler.numthreads"));
		return new BDScheduler(numSysThreads);
	}

	private static BDTableManager createTableManager() {
		return new BDTableManager();
	}

	/**
	 * Initializes the system, loading the database stored in the file
	 * <code>xmlFile</code> into the current system.
	 * 
	 * @param xmlFile
	 *            The database to load into the system
	 * @throws ClassNotFoundException
	 */
	protected static void initSystemResources(String xmlFile) throws ClassNotFoundException {
		FileInputStream propFile = null;
        Properties p = null;
        try {
            propFile = new FileInputStream(DB_CONF);
            p = new Properties(System.getProperties());
            p.load(propFile);
            System.setProperties(p);
            propFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }   

        BDSystem.tableManager = createTableManager();
        try {
			BDSystem.tableManager.readFromXmlFile(xmlFile);
		} catch (XmlException e) {
			System.err.println(e.getLocalizedMessage());
			e.printStackTrace();
			System.err.println("Cannot load file " + xmlFile + ", exiting...");
			System.exit(1);
			
		}
        BDSystem.memoryManager = createMemoryManager();
        BDSystem.diskManager = createDiskManager();
        BDSystem.concurrencyController = createConcurrencyController();
        BDSystem.logManager = createLogManager();
        BDSystem.scheduler = createScheduler();	
	}
	
	public static void main(String[] args) throws ClassNotFoundException {	
        Thread.currentThread().setName("main");

        // Initialize log4j logger
        PropertyConfigurator.configure(LOGGER_CONF);
        
        if(args.length > 0 ) {
        	initSystemResources(args[0]);
        } else {
        	initSystemResources("test/files/Bank.xml");
        }
               
        Thread schedThread = new Thread(BDSystem.scheduler, "sched");
        schedThread.start();	
                                
        // Initialize and start server
        logger.info("Staring server...");
        Thread netThread = new Thread("net") {
            public void run() {
                new BDServer().startDbServer();
            }
        };
        netThread.start();	
        
		//
		// PAVLO HACK: Fire off a thread to run a simulation
		// test for a given input file
		//
		if (System.getProperty("pavlo.testFile") != null && 
		    !System.getProperty("pavlo.testFile").equals("${pavlo.testFile}")) {
			Thread testThread = new Thread() {
				public void run() {
					try {
						while (BDSimulator.visualizer == null) {
							Thread.sleep(1000);
						}
						// Make sure we give time for things to set up...
						Thread.sleep(4000);
						BDSimulator.visualizer.loadTestScript(System.getProperty("pavlo.testFile"));
					} catch (Exception ex) {
						ex.printStackTrace();
						System.exit(-1);
					}
					//
					// Quit when the simulation is done so that we can run another test
					//
					System.exit(0);
				} // RUN
			};
			testThread.start();
		} // PAVLO
        
		// Wait for other threads
        try {
        	schedThread.join();
			netThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
