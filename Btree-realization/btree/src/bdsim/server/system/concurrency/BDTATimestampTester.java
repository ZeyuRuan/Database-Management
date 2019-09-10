package bdsim.server.system.concurrency;

import java.util.*;

import bdsim.server.system.*;

/**
 * Tests various components of the concurrency infrastructure
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu> (12/08/2008)
 */
public class BDTATimestampTester {
   /**
    * This random number generator should be seeded exactly the same
    * for all student tests
    */
   protected static final Random rand = new Random(1981);
   
   protected static final Integer THREAD_SLEEP = 500;
   protected static final Integer NUM_OF_THREADS = 5;
   
   protected boolean pass = true;
   protected int phase = 0;
   
   protected final List<BDSystemThread> threads = new ArrayList<BDSystemThread>();
   
   /**
    * 
    */
   public BDTATimestampTester() {
      System.setProperty("visualizer.doShow", "false");
      System.setProperty("bplustree.d", "6");
      System.setProperty("controller.delay", "1000");
      
      BDSystem.diskManager = new BDDiskManager();
      BDSystem.scheduler = new BDScheduler(100);
      BDSystem.tableManager = new BDTableManager();
   }
   
   public void pass() {
      this.pass = true;
      System.out.println(this.phaseTitle("Test whether Gal Peleg has a body odor problem"));
      System.out.println("He does! Man, that sucks for him...");
   }
   
   public void fail() {
      System.out.println("Failed to perform correct actions for all tests");
      this.pass = false;
   }
   
   private final class TestThread extends BDSystemThread {
      protected final BDTuple tuple;
      
      public TestThread(int tid, BDTuple tuple) {
         super(tid);
         this.tuple = tuple;
      }
      
      public void run() {
         //
         // READ-WRITE TEST
         //
         System.out.println(BDTATimestampTester.this.phaseTitle("Read-Write Test"));
         tuple.setWriteTimestamp(10000);
         Boolean result = null;
         try {
            BDSystem.concurrencyController.readDataItem(tuple);
            result = true;
         } catch (RollbackException ex) {
            System.out.println("Correctly rolled back read!");
         }
         if (result != null) {
            System.out.println("ERROR: Failed to rollback read!");
            BDTATimestampTester.this.pass = false;
         }

         //
         // WRITE-READ TEST
         //
         System.out.println(BDTATimestampTester.this.phaseTitle("Write-Read Test"));
         tuple.setReadTimestamp(10000);
         result = null;
         try {
            BDSystem.concurrencyController.writeDataItem(tuple);
            result = true;
         } catch (RollbackException ex) {
            System.out.println("Correctly rolled back write!");
         }
         if (result != null) {
            System.out.println("ERROR: Failed to rollback write!");
            BDTATimestampTester.this.pass = false;
         }
         
         //
         // WRITE-WRITE TEST
         //
         System.out.println(BDTATimestampTester.this.phaseTitle("Write-Write Test"));
         tuple.setWriteTimestamp(10000);
         tuple.setReadTimestamp(1);
         result = null;
         try {
            result = BDSystem.concurrencyController.writeDataItem(tuple);
         } catch (RollbackException ex) {
            System.out.println("ERROR: Incorrectly rolled back write!");
            BDTATimestampTester.this.pass = false;
         }
         if (result == null) {
            System.out.println("ERROR: Invalid return result from write!");
            BDTATimestampTester.this.pass = false;
         } else if (result) {
            System.out.println("ERROR: Allowed invalid write to go through!");
            BDTATimestampTester.this.pass = false;
         }
      }
   } // END CLASS
   
   /**
    * 
    */
   public void run() throws InterruptedException {
      //
      // TABLES
      //
      System.out.println(this.phaseTitle("Creating test table"));

      String table_name = "Don_Quixote_Pimptoe";
      Vector<String> schema_names = new Vector<String>();
      schema_names.add("col");
      Vector<BDObjectType> schema_types = new Vector<BDObjectType>();
      schema_types.add(BDObjectType.INTEGER);
      BDSchema schema = new BDSchema(schema_names, schema_types);
      BDTable table = new BDTable(schema, "col", table_name);
         
      if (table == null) {
         System.err.println("ERROR: Unable to create test table '" + table_name + "'");
         System.exit(1);
      }
      BDSystem.tableManager.createTable(table_name, table);
      BDTuple tuple = new BDTuple(schema);
      
      //
      // CONCURRENCY CONTROLLER
      //
      BDSystem.concurrencyController = new TimeStampController();
      System.out.println(this.phaseTitle("Starting " + BDSystem.concurrencyController.getClass().getSimpleName()));
      new Thread(BDSystem.scheduler, "sched").start();

      //
      // TEST THREAD!
      //
      BDSystemThread thread = new BDTATimestampTester.TestThread(100, tuple);
      thread.start();
      thread.join();
      
      if (!this.pass) {
         this.fail();
      } else {
         this.pass();
      }
   }
   
   protected void printException(Exception ex) {
      final java.io.Writer result = new java.io.StringWriter();
      final java.io.PrintWriter printWriter = new java.io.PrintWriter(result);
      ex.printStackTrace(printWriter);
      System.out.println("============================================================\n" + 
                         result.toString() +
                         "============================================================");
   }
   
   protected String phaseTitle(String title) {
      return ("PHASE" + ++this.phase + ": " + title);
   }

}
