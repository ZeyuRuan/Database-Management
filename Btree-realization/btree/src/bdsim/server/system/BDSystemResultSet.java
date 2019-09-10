package bdsim.server.system;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import bdsim.common.BDRow;
import bdsim.common.BDResultSet;
import bdsim.server.system.concurrency.RollbackException;
import bdsim.server.system.index.BDIndex.RangeType;

/**
 * @author dclee
 * @revision $Id: BDSystemResultSet.java 304 2007-01-22 19:55:30 +0000 (Mon, 22 Jan 2007) wpijewsk $
 */
public class BDSystemResultSet implements BDResultSet, Serializable {

	private static final long serialVersionUID = -8741784965412683837L;
	private List<String> m_columns;
	private List<BDTuple> m_rows;
	private BDSchema m_schema;
	
	public BDSystemResultSet() {
		m_rows = new LinkedList<BDTuple>();
	}

	/**
	 * Adds a row to the current result set at the end of result set. Note: this
	 * method uses a copy constructor to make a deep copy of the tuple.
	 * 
	 * @throws RollbackException
	 */
	public void addRow(BDTuple tuple) throws RollbackException {
		if(m_schema == null) {
			m_schema = tuple.getSchema();
		} 
		
		assert(m_schema == tuple.getSchema());
		
		synchronized(tuple) {
			// Check row's read timestamp
			BDSystem.concurrencyController.readDataItem(tuple);
			// Use copy constructor to make deep copy
			m_rows.add(new BDTuple(tuple));
		}
	}
	
	public void addRowWithoutCopy(BDTuple tuple) {
		if(m_schema == null) {
			m_schema = tuple.getSchema();
		} 
		assert(m_schema == tuple.getSchema());
				
		m_rows.add(tuple);
	}

	public List<String> getColumns() {
		return m_columns;
	}
	
	public List<? extends BDRow> getData() {
		return m_rows;
	}
	
	public int getMemorySize() {
		return m_rows.get(0).getSchema().getMemorySize() * m_rows.size(); 
	}
	
	public int getNumTuples() {
		return m_rows.size();
	}
	
	public BDSchema getSchema() {
		if(m_rows.size() == 0) return null;
		return m_rows.get(0).getSchema(); 
	}
	
	public List<BDTuple> getTupleData() {
		return m_rows;
	}
	
	@SuppressWarnings("unchecked")
	public BDSystemResultSet getTuplesByRange(RangeType rtype, String field, Comparable value) throws RollbackException {
		BDSystemResultSet result = new BDSystemResultSet();
		for (BDTuple t : this.getTupleData()) {
			switch (rtype) {
			case EQ:
				if (((Comparable) t.getObject(field)).compareTo(value) == 0)
					result.addRow(t);
				break;
			case NEQ:
				if (((Comparable) t.getObject(field)).compareTo(value) != 0)
					result.addRow(t);
				break;
			case LT:
				if (((Comparable) t.getObject(field)).compareTo(value) < 0)
					result.addRow(t);
				break;
			case LTEQ:
				if (((Comparable) t.getObject(field)).compareTo(value) <= 0)
					result.addRow(t);
				break;
			case GT:
				if (((Comparable) t.getObject(field)).compareTo(value) > 0)
					result.addRow(t);
				break;
			case GTEQ:
				if (((Comparable) t.getObject(field)).compareTo(value) >= 0)
					result.addRow(t);
				break;
			}
			
		}		
		return result;
	}
	
	/*
	 * No index here: Brute force methods
	 */
	@SuppressWarnings("unchecked")
	public BDSystemResultSet getTuplesByValue(String field, Comparable value) throws RollbackException {
		BDSystemResultSet result = new BDSystemResultSet();		
		for(BDTuple t : this.getTupleData()) {
			if(((Comparable)t.getObject(field)).compareTo(value) == 0) {
				result.addRow(t);
			}
		}		
		return result;
	}

	public boolean hasTuple(BDTuple t2) {
		for(BDTuple t : this.m_rows) {
			if(t2.equals(t)) {
				return true;
			}
		}
		return false;
	}

	public void remove(BDTuple t) {
		this.m_rows.remove(t);
	}
	
	public void setSchema(BDSchema schema) {
		this.m_schema = schema;
	}

	@SuppressWarnings("unchecked")
	public void sort(boolean ascending) {
		Collections.sort(m_rows);
		if(!ascending) Collections.reverse(m_rows); 
	}
	
	public String toString() {
		String stuff = new String();
		for(BDTuple t : m_rows) {
			stuff += t;
			stuff += "\n";
		}
		return stuff;
	}
	
}