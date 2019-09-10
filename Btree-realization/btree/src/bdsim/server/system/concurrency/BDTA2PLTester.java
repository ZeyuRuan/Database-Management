package bdsim.server.system.concurrency;

import java.util.*;

import bdsim.server.system.*;

/**
 * Tests various components of the concurrency infrastructure
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu> (12/08/2008)
 */
public class BDTA2PLTester {
   /**
    * This random number generator should be seeded exactly the same
    * for all student tests
    */
   protected static final Random rand = new Random(1981);
   
   protected static final Integer THREAD_SLEEP = 500;
   protected static final Integer CHECKER_SLEEP = 20000;
   
   protected static final Integer NUM_OF_TABLES = 3;
   protected static final Integer NUM_OF_THREADS = 2;
   
   protected boolean pass = false;
   protected int phase = 0;
   
   protected final List<BDSystemThread> threads = new ArrayList<BDSystemThread>();
   protected Thread scheduler = null;
   
   /**
    * 
    */
   public BDTA2PLTester() {
      System.setProperty("visualizer.doShow", "false");
      System.setProperty("bplustree.d", "6");
      System.setProperty("controller.delay", "1000");
      
      BDSystem.diskManager = new BDDiskManager();
      BDSystem.scheduler = new BDScheduler(100);
      BDSystem.tableManager = new BDTableManager();
   }
   
   public void pass() {
      if (!this.pass) {
         System.out.println("Finished test");
         this.pass = true;
         System.out.println("TEST RESULT: PASS");
         System.exit(BDTATester.final_result);
      }
      this.scheduler.interrupt();
   }
   
   public void fail() {
      System.out.println("Failed to find deterministic deadlock!");
      this.pass = false;
      System.out.println("TEST RESULT: FAIL");
      System.exit(1);
   }
   
   private final class TestThread extends BDSystemThread {
      protected final List<String> tables;
      
      public TestThread(int tid, List<String> tables) {
         super(tid);
         this.tables = tables;
         
      }
      
      public void run() {
         try {
            //
            // We want to lock each thread one at a time and then sleep 
            // for a bit to make sure that other threads can run.
            //
            for (String table : this.tables) {
               if (BDTA2PLTester.this.pass) break;
               System.out.println(this.getName() + " is acquiring write lock on table " + table);
               BDSystem.concurrencyController.lockTableForWriting(table);
               sleep(THREAD_SLEEP);
               yield();
            } // FOR
            
            //
            // Now that we have all of our locks, we just need to blast through and release them
            //
            for (String table : this.tables) {
               if (BDTA2PLTester.this.pass) break;
               System.out.println(this.getName() + " is unlocking table " + table);
               BDSystem.concurrencyController.unlockTable(table);
               yield();
            } // FOR
         //
         // At least one of the threads should get rolled back!
         //
         } catch (RollbackException e) {
            if (!BDTA2PLTester.this.pass) {
               System.out.println("Correctly rolled back thread: " + this.getName());
               BDTA2PLTester.this.pass();
            }
            BDTA2PLTester.this.pass = true;
            return;
         //
         // Everything else should be an error
         //
         } catch (Exception ex) {
            BDTA2PLTester.this.printException(ex);
            BDTA2PLTester.this.pass = false;
            return;
         }
      }
   } // END CLASS
   
   /**
    * 
    */
   public final void run() throws InterruptedException {
      //
      // TABLES
      //
      System.out.println(this.phaseTitle("Creating " + NUM_OF_TABLES + " tables"));
      List<String> tables = new ArrayList<String>();
      for (int ctr = 0; ctr < NUM_OF_TABLES; ctr++) {
         String table_name = "Table" + ctr;
         
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
         tables.add(table_name);
      } // FOR
      
      //
      // CONCURRENCY CONTROLLER
      //
      BDSystem.concurrencyController = new TwoPhaseLockController();
      System.out.println(this.phaseTitle("Starting " + BDSystem.concurrencyController.getClass().getSimpleName()));
      this.scheduler = new Thread(BDSystem.scheduler, "sched");
      this.scheduler.start();
      
      //
      // THREADS
      // Notice that we reverse the list of locks each time so that the threads
      // try to acquire the locks in different orders
      //
      System.out.println(this.phaseTitle("Creating " + NUM_OF_THREADS + " threads"));
      for (int ctr = 0; ctr < NUM_OF_THREADS; ctr++) {
         List<String> new_tables = new ArrayList<String>();
         new_tables.addAll(tables);
         
         int tid = 100 + ctr;
         BDSystemThread thread = new BDTA2PLTester.TestThread(tid, new_tables);
         BDSystem.scheduler.addThread(thread);
         
         this.threads.add(thread);
         thread.start();
         
         Collections.reverse(tables);
      } // FOR
      
      //
      // CATCH THREAD
      // We fire off an extra thread that will wait on a timer. If no 
      // threads have been caught in the deadlock, then we know that their crap is broken!
      //
      new Thread() {
         @Override
         public void run() {
            System.out.println("Starting deadlock checker thread...");
            try {
               sleep(CHECKER_SLEEP); // long enough...
            } catch (Exception ex) {
               ex.printStackTrace();
            }
            BDTA2PLTester.this.fail();
         }
      }.start();
      
      //
      // JOIN
      //
      for (Thread thread : threads) {
         thread.join();
      } // FOR
      
      if (!this.pass) {
         System.err.println("Threads completed without deadlock! This should never happen!");
         this.fail();
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
   
   public static void main(String[] args) {
      //
      // Execute the tester!! Suck it!
      //
      boolean pass = false;
      try {
         new BDTA2PLTester().run();
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      System.exit(pass ? 0 : 1);
   }
}
