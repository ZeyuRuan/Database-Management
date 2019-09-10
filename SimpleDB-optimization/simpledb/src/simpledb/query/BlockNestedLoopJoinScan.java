package simpledb.query;

import simpledb.multibuffer.ChunkScan;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class BlockNestedLoopJoinScan implements Scan {
    
    private String fldname1, fldname2;
    private TableInfo ti1;
    private TableInfo ti2;
    private Transaction tx;
    private int outerblknum, innerblknum, filesize1, filesize2;

    private Scan outerBlock;
    private Scan innerBlock;
    private Scan nestedLoopScan;

    public BlockNestedLoopJoinScan(TableInfo ti1, TableInfo ti2, String fldname1, String fldname2, Transaction tx) {
        this.ti1 = ti1;
        this.ti2 = ti2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.tx = tx;
        this.filesize1 = tx.size(ti1.fileName());
        this.filesize2 = tx.size(ti2.fileName());
        beforeFirst();
    }
    
    @Override
    public void beforeFirst() {
    	outerblknum = 0;
    	innerblknum = 0;
        getNextInnerJoin();
    }

    /**
     * Position the iterator to the next valid tuple in the join
     * @return true if there is a next tuple, false otherwise.
     */
    @Override
    public boolean next() {
    	// TODO: Get the next tuple using the block nested loop join algorithm
    	
    	if(this.nestedLoopScan.next())
    		return true;
    	else{
    		if(getNextInnerJoin()){
    			this.nestedLoopScan.next();
    			return true;
    		}
    	}
    	
    	return false;
    }

    @Override
    public void close() {
        nestedLoopScan.close();
    }

    @Override
    public Constant getVal(String fldname) {
    	return nestedLoopScan.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
    	return nestedLoopScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
    	return nestedLoopScan.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
    	return nestedLoopScan.hasField(fldname);
    }

    /**
     * Gets the next block of the outer relation
     * @return Scan object of the block, or null if there are no more blocks left
     */
    private Scan getNextOuterBlock() {
        if (outerblknum >= filesize1)
            return null;
        Scan s = new ChunkScan(ti1, outerblknum, outerblknum, tx);
        outerblknum++;
        return s;
    }

    /**
     * Gets the next block of the inner relation
     * @return Scan object of the block
     */
    private Scan getNextInnerBlock() {
        if (innerBlock != null)
            innerBlock.close();
        Scan s = new ChunkScan(ti2, innerblknum, innerblknum, tx);
        innerblknum++;
        return s;
    }


    /**
     * Gets the next inner nested loop join.
     * @return true if successfully found the next nested loop join
     */
    private boolean getNextInnerJoin() {
        // TODO: Get the next outer and/or inner blocks to join.
        // Hint: use the helper methods getNextOuterBlock(), getNextInnerBlock()
    	
    	innerBlock = getNextInnerBlock();
    	if(outerblknum==filesize1 && innerblknum==filesize2)
    		return false;
    	if(innerblknum==filesize2){
    		innerBlock.beforeFirst();
    		outerBlock=getNextOuterBlock();
    	}
    	
        nestedLoopScan = new NestedLoopJoinScan(outerBlock, innerBlock, fldname1, fldname2);
        return true;
    }
}
