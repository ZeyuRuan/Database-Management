package simpledb.query;

import simpledb.tx.Transaction;
import simpledb.materialize.TempTable;
import simpledb.record.*;

public class BlockNestedLoopJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();
    private Transaction tx;
    TableInfo ti1, ti2;

    public BlockNestedLoopJoinPlan(Plan p1, Plan p2, String fldname1, String fldname2, Transaction tx) {
      this.p1 = p1;
      this.p2 = p2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
      this.tx = tx;
      
      TempTable tt1 = copyRecordsFrom(p1);
      ti1 = tt1.getTableInfo();
      TempTable tt2 = copyRecordsFrom(p2);
      ti2 = tt2.getTableInfo();
      int filesize1 = tx.size(ti1.fileName());
      int filesize2 = tx.size(ti2.fileName());
      
      if (filesize1 < filesize2) {
    	  TableInfo tempti = ti1;
    	  ti1 = ti2;
    	  ti2 = tempti;
    	  String tempfld = fldname1;
    	  this.fldname1 = fldname2;
    	  this.fldname2 = tempfld;
      }
    }

    public Scan open() {
        return new BlockNestedLoopJoinScan(ti1, ti2, fldname1, fldname2, tx);
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
    
    private TempTable copyRecordsFrom(Plan p) {
        Scan src = p.open(); 
        Schema sch = p.schema();
        TempTable tt = new TempTable(sch, tx);
        UpdateScan dest = (UpdateScan) tt.open();
        while (src.next()) {
           dest.insert();
           for (String fldname : sch.fields())
              dest.setVal(fldname, src.getVal(fldname));
        }
        src.close();
        dest.close();
        return tt;
    }
}