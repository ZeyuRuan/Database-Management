package bdsim.server.system;

import java.util.Vector;

import bdsim.server.system.concurrency.BDTrackableReadWriteLock;
import bdsim.server.system.concurrency.RollbackException;
import bdsim.server.system.index.BDBPlusTreeIndex;
import bdsim.server.system.index.BDIndex;
import bdsim.server.system.index.BDIndex.RangeType;

/**
 * @author dclee
 * @revision $Id: BDTable.java 301 2007-01-22 18:32:38 +0000 (Mon, 22 Jan 2007) acath $
 */
public class BDTable {
	private BDBPlusTreeIndex m_primaryIndex;
	private BDSchema m_schema;
	private Vector<BDIndex> m_secondaryIndices;
	private String m_primaryIndexName;
	private BDTrackableReadWriteLock<BDSystemThread> m_lock;
	private int m_numTuples;
	private String m_name;
	
	public BDTable(BDSchema schema, String primaryName, String table) {
		m_numTuples = 0;
		m_schema = schema;
		m_primaryIndexName = primaryName;
		m_primaryIndex = new BDBPlusTreeIndex(this, 
				Integer.parseInt(System.getProperty("bplustree.d")), 
				m_primaryIndexName, true);
		m_secondaryIndices = new Vector<BDIndex>();
		m_lock = new BDTrackableReadWriteLock<BDSystemThread>(table);
		m_name = table;
	}

	public void buildIndexOnAttribute(String columnName) {	
		m_secondaryIndices.add(new BDBPlusTreeIndex(this, 
				Integer.parseInt(System.getProperty("bplustree.d")), 
				columnName, false));
	}
	
	public void commit(int TID) throws InterruptedException, RollbackException {
		m_primaryIndex.commit(TID);
		for (BDIndex index : m_secondaryIndices) {
			index.commit(TID);
		}
	}
	public void delete(BDTuple t) throws InterruptedException {

		m_primaryIndex.delete(t);
		for (BDIndex index : m_secondaryIndices) {
			index.delete(t);
		}
		m_numTuples--;
	}
	
	public BDSystemResultSet getAllTuples() throws InterruptedException, RollbackException {
		return m_primaryIndex.getAllTuples();
	}
	
	public BDSystemResultSet getAllTuplesUnchecked() {
		return m_primaryIndex.getAllTuplesUnchecked();
	}
	
	public BDIndex getIndex(String columnName) {
		if (m_primaryIndex.getKeyName().equals(columnName))
			return m_primaryIndex;

		for (BDIndex index : m_secondaryIndices)
			if (index.getKeyName().equals(columnName))
				return index;
		return null;
	}
	
	
	public BDIndex getPrimaryIndex() {
		return m_primaryIndex;
	}
    
    public boolean isPrimaryIndex(String column) {
		if (m_primaryIndex == null) {
			return false;
		} else {
			return m_primaryIndex.getKeyName().equals(column);
		}
	}
    
    public boolean isSecondaryIndex(String column) {
		for (BDIndex index : m_secondaryIndices) {
			if (index.getKeyName().equals(column)) {
				return true;
			}
		}  
        return false;
    }

	public BDSchema getSchema() {
		return m_schema;
	}

	/**
	 * @return The lock for this table
	 */
	public synchronized BDTrackableReadWriteLock<BDSystemThread> getTableLock() {
		return m_lock;
	}
    
	/**
	 * @return  The number of tuples in this table
	 */
    public int getTupleCount() {
		return m_numTuples;
	}
    
    public BDSystemResultSet getTuplesByRange(RangeType rtype,
			String columnName, Object value) throws InterruptedException, RollbackException {
		
		BDIndex index = getIndex(columnName);
		if(index == null) index = m_primaryIndex;
		//if (index == null) {
			//TODO Brute force search not yet implemented
		//	return new BDSystemResultSet();
		//}
		return index.getTuplesByRange(rtype, columnName, (Comparable) value);
	}

	public BDSystemResultSet getTuplesByValue(String columnName, Object value) throws InterruptedException, RollbackException {
		BDIndex index = getIndex(columnName);
		
		if(index == null) index = m_primaryIndex;
		//if (index == null) {
			//TODO Brute force search not yet implemented
		//	return new BDSystemResultSet();
		//}
		return index.getTuplesByValue(columnName, (Comparable) value);
	}

	public boolean hasIndexForAttribute(String columnName) {
		return false;
	}

	public void insert(BDTuple t) throws InterruptedException {
		m_primaryIndex.insert(t);
		for (BDIndex index : m_secondaryIndices) {
			index.insert(t);
		}

		m_numTuples++;
	}

	/**
	 * A unsafe version of insert - does not check for any concurrency. Note:
	 * this only inserts the tuple in the primary index.
	 * 
	 * @param t
	 *            The tuple to insert
	 */
	public void insertUnsafe(BDTuple t) throws InterruptedException {
		m_primaryIndex.insertNow(t);
	}

	public boolean isPrimaryKey(String columnName) {
		return columnName.equals(m_primaryIndex.getKeyName());
	}

	/**
	 * Replaces an old tuple with a new tuple.
	 * 
	 * @param oldTuple
	 *            The tuple to be replaced
	 * @param newTuple
	 *            The replacement tuple
	 */
	public void replace(BDTuple oldTuple, BDTuple newTuple) {
		m_primaryIndex.replace(oldTuple, newTuple);
		for (BDIndex index : m_secondaryIndices) {
			index.replace(oldTuple, newTuple);
		}
	}
		
	

	public void rollback(int TID) {
		m_primaryIndex.rollback(TID);
		for (BDIndex index : m_secondaryIndices) {
			index.rollback(TID);
		}
	}

	/**
	 * @return  The name of this table
	 */
	public String getName() {
		return m_name;
	}

	/**
	 * Removes a tuple from a pending transaction
	 * @param tuple  The tuple to remove
	 */
	public void abandon(BDTuple tuple) {
		m_primaryIndex.abandon(tuple);
		for (BDIndex index : m_secondaryIndices) {
			index.abandon(tuple);
		}
	}
}
