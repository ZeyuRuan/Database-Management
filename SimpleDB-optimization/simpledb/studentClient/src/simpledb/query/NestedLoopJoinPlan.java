package simpledb.query;

import simpledb.tx.Transaction;
import simpledb.record.*;

public class NestedLoopJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();

    public NestedLoopJoinPlan(Plan p1, Plan p2, String fldname1, String fldname2, Transaction tx) {
      this.p1 = p1;
      this.p2 = p2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
    }

    public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      return new NestedLoopJoinScan(s1, s2, fldname1, fldname2);
    }

    public int blocksAccessed() {
        // TODO: Return the number of blocks this will access in terms of the input relations
      return -1;
    }

    public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
                             p2.distinctValues(fldname2));
      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
    }

    public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
    }

    public Schema schema() {
      return sch;
   }
}