package bdsim.server.system.index;

import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee
 * @revision $Id: BDIndex.java 285 2007-01-20 21:16:47 +0000 (Sat, 20 Jan 2007) acath $
 */
public interface BDIndex {

	public enum IndexType {
		B_PLUS_TREE, HASH_TABLE, ISAM, NO_INDEX
	};

	public enum RangeType {
		EQ, GT, LT, GTEQ, LTEQ, NEQ, LIKE
	};

	public IndexType getIndexType();

	public void replace(BDTuple oldTuple, BDTuple newTuple);

	public void delete(BDTuple t) throws InterruptedException;

	public void abandon(BDTuple tuple);
	
	public void insert(BDTuple t) throws InterruptedException;

	public String getKeyName();

	public BDSystemResultSet getAllTuples() throws InterruptedException, RollbackException;

	// NON-CONCURRENCY-CHECKED VERSION
	public BDSystemResultSet getAllTuplesUnchecked();

	public BDSystemResultSet getTuplesByRange(RangeType rtype, String field,
			Comparable value) throws InterruptedException, RollbackException;

	public BDSystemResultSet getTuplesByValue(String field, Comparable value)
			throws InterruptedException, RollbackException;

	public void commit(int tid) throws InterruptedException, RollbackException;

	public void rollback(int tid);
}
