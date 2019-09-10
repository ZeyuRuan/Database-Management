package bdsim.server.system;

import java.io.Serializable;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import bdsim.common.BDRow;

/**
 * The internal representation of the tuples in the database.
 * 
 * @author dclee, wpijewsk
 * @revision $Id: BDTuple.java 293 2007-01-21 16:28:18 +0000 (Sun, 21 Jan 2007) wpijewsk $
 */
public class BDTuple extends BDRow implements Comparable, Serializable {

	private static final long serialVersionUID = 7550947941298359075L;
	private String m_tempKey;
	private int m_readTimestamp;
	private int m_writeTimestamp;
	private Lock m_lock;

	/**
	 * Class constructor.
	 * 
	 * @param schema
	 *            The schema that this tuple is based on.
	 */
	public BDTuple(BDSchema schema) {
		super(schema);
		m_fields = new Vector<Object>();
		m_fields.setSize(schema.size());
		m_tempKey = null;
		m_readTimestamp = Integer.MIN_VALUE;
		m_writeTimestamp = Integer.MIN_VALUE;
		m_lock = new ReentrantLock();
	}
	
	/**
	 * Copy constructor
	 * @param tuple  The tuple to copy
	 */
	@SuppressWarnings("unchecked")
	public BDTuple(BDTuple tuple) {
		super(new BDSchema(tuple.m_schema));
		m_readTimestamp = tuple.m_readTimestamp;
		m_writeTimestamp = tuple.m_writeTimestamp;
		m_tempKey = tuple.m_tempKey;
		m_fields = (Vector<Object>)tuple.m_fields.clone();
	}

	@SuppressWarnings("unchecked")
	public int compareTo(Object other) {
		if (m_tempKey == null)
			throw new ClassCastException(
					"Comparing tuple when temp key is not initialized");
		BDTuple t;
		if (!(other instanceof BDTuple)) {
			throw new ClassCastException(
					"Comparing tuple to something that's not");
		}
		t = (BDTuple) other;

		if (m_tempKey != t.getTempKey())
			throw new ClassCastException("Comparing tuple on invalid key");

		if (this.getField(m_tempKey) == null
				&& ((BDTuple) t).getField(m_tempKey) == null) {
			return 0;
		}

		if (this.getField(m_tempKey) == null) {
			return 1;
		}

		if (((BDTuple) t).getField(m_tempKey) == null) {
			return -1;
		}

		return ((Comparable) (this.getField(m_tempKey)))
				.compareTo((Comparable) (((BDTuple) t).getField(m_tempKey)));
	}

	@SuppressWarnings("unchecked")
	public boolean equals(Object other) {
		BDTuple t;
		if (!(other instanceof BDTuple)) {
			return false;
		}
		t = (BDTuple) other;
		if (t.getNumCols() != this.getNumCols())
			return false;
		try {
			for (int i = 0; i < getNumCols(); i++) {
				if (((Comparable) (t.getObjects().elementAt(i)))
						.compareTo(((Comparable) getObjects().elementAt(i))) != 0)
					return false;
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	public String getName(int position) {
		return m_schema.getName(position);
	}

	public int getNumCols() {
		return m_fields.size();
	}

	/**
	 * @param index
	 *            The column to be read.
	 * @return The value in that column.
	 */
	public Object getObject(int index) {
		return m_fields.elementAt(index);
	}

	/** 
	 * @param column
	 *            The column to be read
	 * @return The value in that column
	 */
	public Object getObject(String column) {
		return m_fields.elementAt(m_schema.getPosition(column));
	}
	
	/**
	 * @return All objects in this tuple
	 */
	public Vector<Object> getObjects() {
		return m_fields;
	}

	public BDObjectType getObjectType(int position) {
		return m_schema.getObjectType(position);
	}

	public int getReadTimestamp() {
		return m_readTimestamp;
	}

	/**
	 * @return The schema that this tuple is based on.
	 */
	public BDSchema getSchema() {
		return m_schema;
	}

	public String getTempKey() {
		return m_tempKey;
	}

	public int getWriteTimestamp() {
		return m_writeTimestamp;
	}
	
	/**
	 * @param index
	 *            The index at which the item should be inserted
	 * @param obj
	 *            The item to insert at the specified column
	 */
	public void setObject(int index, Object obj) {
		m_fields.set(index, obj);
	}
	
	/**
	 * @param index
	 *            The column in which the item should be inserted
	 * @param obj
	 *            The item to insert at the specified column
	 */
	public void setObject(String column, Object obj) {
		m_fields.set(m_schema.getPosition(column), obj);
	}
	
	/**
	 * Sets the new read timestamp
	 * 
	 * @param timestamp
	 *            The new read timestamp
	 */
	public void setReadTimestamp(int timestamp) {
		m_readTimestamp = timestamp;
	}

	public void setTempKey(String tempKey) {
		m_tempKey = tempKey;
	}

	/**
	 * Sets the new write timestamp
	 * 
	 * @param timestamp
	 *            The new write timestamp
	 */
	public void setWriteTimestamp(int timestamp) {
		m_writeTimestamp = timestamp;
	}
	
	public String toString() {
		String returnString = new String();
		for (Object o : m_fields) {
			if (o == null) {
				returnString += "NULL";
			} else {
				returnString += o.toString();
			}
			returnString += ";";
		}
		return returnString;
	}

	public void lock() {
		m_lock.lock();		
	}

	public void unlock() {
		m_lock.unlock();
	}
	
	
}