package bdsim.server.system.log;

import bdsim.server.system.BDTuple;

import java.util.*;
import java.io.*;

/**
 * @author pavlo
 */
public final class BDStudentLogManager implements BDLogManager {
   protected static Integer TRANSACTION_ID = 0;
   protected int checkpoint_id = 0;
   
   //
   // The key of this hashtable is the thread id.
   // It points to a list of transaction objects
   // The first object in this list is either the active one or already committed
   // All other transactions in the list are assumed to be committed and waiting
   // to be checkpointed
   //
   protected Hashtable<Integer, List<BDStudentLogManager.BDTransaction>> activeLog;
   
	/**
	 * Class constructor.
	 */
	public BDStudentLogManager() {
      this.activeLog = new Hashtable<Integer, List<BDStudentLogManager.BDTransaction>>();
 	}
	
	/**
	 * @see bdsim.server.system.log.BDLogManager#checkpoint()
	 */
	public void checkpoint() {
      //
      // Block on the log, and then write everything out into a new log file
      //
      synchronized (this) {
         List<BDTransaction> log = new LinkedList<BDTransaction>();
         List<Integer> removeThreads = new LinkedList<Integer>();
         //
         // Flush out all committed transactons to the "disk"
         //
         for (Integer idx : this.activeLog.keySet()) {
            //
            // Loop through all the transactions for this thread
            //
            List<BDTransaction> remove = new LinkedList<BDTransaction>();
            for (BDTransaction xaction : this.activeLog.get(idx)) {
               if (xaction.isCommitted()) {
                  log.add(xaction);
                  remove.add(xaction);
               }
            } // FOR
            //
            // Remove the transactions for this thread
            //
            for (BDTransaction xaction : remove) {
               this.activeLog.get(idx).remove(xaction);
            }
            //
            // If the thread transaction list is empty, we can remove the idx too
            //
            if (this.activeLog.get(idx).isEmpty()) removeThreads.add(idx);
         } // FOR
         //
         // At this point we would write the checkpoint out to the disk, but
         // we don't need to do that here
         // So we just remove what we need from the activeLog
         //
         for (Integer idx : removeThreads) {
            this.activeLog.remove(idx);
         } // FOR
         //
         // Update our current checkpoint_id now that we are done
         //
         this.checkpoint_id++;
      } // SYNCHRONIZED
	}

	/**
	 * @see bdsim.server.system.log.BDLogManager#commitTransaction()
	 */
	public void commitTransaction() {
      synchronized (this) {
         //
         // Get the transaction for this thread
         // It is always the first one in our list
         //
         int thread_id = ((BDLogThread)Thread.currentThread()).getTransactionId();
         if (!this.activeLog.containsKey(thread_id)) {
            System.err.println("ERROR: Thread #" + thread_id + " tried to commit transaction without starting one first!");
            System.exit(-1);
         } else if (this.activeLog.get(thread_id).isEmpty() || this.activeLog.get(thread_id).get(0).isCommitted()) {
            System.err.println("ERROR: Thread #" + thread_id + " tried to commit transaction twice!");
            System.exit(-1);
         }
         this.activeLog.get(thread_id).get(0).commit();
      }
      return;
	}

	/**
	 * @see bdsim.server.system.log.BDLogManager#dataItemWritten(bdsim.server.system.BDTuple, java.lang.String, java.lang.Object, java.lang.Object)
	 */
	public void dataItemWritten(BDTuple tuple, String column, Object oldObj, Object newObj) {
		synchronized (this) {
		   //
         // Get the transaction for this thread
         // Again, remember that the first transaction is always the open one
         //
         int thread_id = ((BDLogThread)Thread.currentThread()).getTransactionId();
         if (!this.activeLog.containsKey(thread_id)) {
            System.err.println("ERROR: Thread #" + thread_id + " tried to write data item without starting a transaction!");
            System.exit(-1);
         } else if (this.activeLog.get(thread_id).isEmpty() || this.activeLog.get(thread_id).get(0).isCommitted()) {
            System.err.println("ERROR: Thread #" + thread_id + " tried to write data after committing the transaction!");
            System.exit(-1);
         }
         this.activeLog.get(thread_id).get(0).write(tuple, column, oldObj, newObj);
      } // SYNCHRONIZED
      return;
	}

	/**
	 * @see bdsim.server.system.log.BDLogManager#recover()
	 */
	public void recover() {
      synchronized (this) {
         //
         // Go through and undo all of our un-checkpointed actions in reverse order
         //
         List<BDAction> actions = this.getSortedActions();
         if (!actions.isEmpty()) {
            Collections.sort(actions, new BDActionComparator(true));
            for (BDAction action : actions) {
               action.undo();
            } // FOR
            //
            // Now of the transactions that were committed, we can redo them
            //
            Collections.sort(actions, new BDActionComparator(false));
            for (BDAction action : actions) {
               if (action.parent.isCommitted()) {
                  action.redo();
               }
            } // FOR
         }
         //
         // Remove all of the transactions so that we don't try to redo/undo
         // them the next time that we come around
         //
         this.activeLog.clear();
      } // SYNCHRONIZED
	}

	/**
	 * @see bdsim.server.system.log.BDLogManager#startTransaction()
	 */
	public void startTransaction() {
      synchronized (this) {
         //
         // Create a new transaction record for this thread
         // TODO: A thread can have multiple transactions before a checkpoint
         //
         int thread_id = ((BDLogThread)Thread.currentThread()).getTransactionId();
         if (!this.activeLog.containsKey(thread_id)) {
            this.activeLog.put(thread_id, new LinkedList<BDTransaction>());
         }
         //
         // Make sure that they don't already have an open transaction
         //
         if (!this.activeLog.get(thread_id).isEmpty() && !this.activeLog.get(thread_id).get(0).isCommitted()) {
            System.err.println("ERROR: Thread #" + thread_id + " tried to start a new transaction before committing the previous one!");
            System.exit(-1);
         }
         this.activeLog.get(thread_id).add(0, new BDTransaction(thread_id));
      } // SYNCHRONIZED
	}
   
   protected List<BDAction> getSortedActions() {
      List<BDAction> ret = new LinkedList<BDAction>();
      //
      // Loop through all our transactions and add in their actions
      // The timestamps will nicely sort themselves
      //
      for (Integer idx : this.activeLog.keySet()) {
         for (BDTransaction xaction : this.activeLog.get(idx)) {
            for (BDAction action : xaction.getActions()) {
               ret.add(action);
            } // FOR
         } // FOR
      } // FOR
      return (ret);
   }
   
   //
   // A transaction contains a list of actions 
   //
   protected class BDTransaction implements Serializable {
      protected final int id;
      protected final Date start_timestamp;
      protected Date commit_timestamp;
      protected final int thread_id;
      protected List<BDStudentLogManager.BDAction> actions;
      
      public BDTransaction(int thread_id) {
         synchronized (BDStudentLogManager.TRANSACTION_ID) {
            this.id = BDStudentLogManager.TRANSACTION_ID++;
         }
         this.thread_id = thread_id;
         this.start_timestamp = Calendar.getInstance().getTime();
         this.actions = new LinkedList<BDAction>();
      }
      
      public synchronized void commit() {
         if (this.isCommitted()) {
            System.err.println("ERROR: Trying to commit transaction " + this + ", but was already committed!");
            System.exit(-1);
         }
         this.commit_timestamp = Calendar.getInstance().getTime();
      }
      
      public List<BDAction> getActions() {
         return (this.actions);
      }
      
      public synchronized void write(BDTuple tuple, String name, Object old_value, Object new_value) {
         this.actions.add(new BDAction(this, tuple, name, old_value, new_value));
      }
      
      public boolean isCommitted() {
         return (this.commit_timestamp != null);
      }
      
      public String toString() {
         return ("Transaction[" + this.id + "|Thread#" + this.thread_id + "]");
      }
   } // END CLASS
   
   //
   // An action that took place in the database
   //
   protected class BDAction implements Serializable, Comparable {
      //
      // It would be nice if we didn't have to store the Tuple object in here,
      // but what are you going to do?
      //
      protected final BDTransaction parent;
      protected final Date timestamp;
      protected final BDTuple tuple;
      protected final String field;
      protected final Object old_value;
      protected final Object new_value;
      
      public BDAction(BDTransaction parent, BDTuple tuple, String name, Object old_value, Object new_value) {
         this.parent    = parent;
         this.timestamp = Calendar.getInstance().getTime();
         this.tuple     = tuple;
         this.field      = name;
         this.old_value = old_value;
         this.new_value = new_value;
      }
      
      protected synchronized void undo() {
         this.tuple.setObject(this.field, this.old_value);
      }
      
      protected synchronized void redo() {
         this.tuple.setObject(this.field, this.new_value);
      }
      
      public int compareTo(Object o) {
         if (o instanceof BDAction) {
            return (this.timestamp.compareTo(((BDAction)o).timestamp));
         }
         return 0;
      }
      
      public boolean equals(Object o) {
         if (o instanceof BDAction) {
            return (this.timestamp.equals(((BDAction)o).timestamp) &&
                    this.tuple.equals(((BDAction)o).tuple));
         }
         return (false);
      }
      
      public String toString() {
         return ("Action[" + this.field + "|" + this.parent + "]");
      }
   } // END CLASS
   
   //
   // Used to sort actions based on timestamps!
   //
   public class BDActionComparator<BDBDAction> implements Comparator {
      protected boolean reverse;
      
      public BDActionComparator(boolean reverse) {
         this.reverse = reverse;
      }
      
      public BDActionComparator() {
         this(false);
      }
      
      public int compare(Object o1, Object o2) {
         if (o1 instanceof BDAction && o2 instanceof BDAction) {
            return (((BDAction)o1).compareTo(o2) * (this.reverse ? -1 : 1));
         }
         return (0);
      }
  } // END CLASS
}
