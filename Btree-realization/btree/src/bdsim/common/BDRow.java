package bdsim.common;

import java.io.Serializable;
import java.util.Vector;

import bdsim.server.system.BDSchema;

/**
 * Represents a single row in a result set.
 * @author wpijewsk
 * @revision $Id: BDRow.java 274 2007-01-20 02:56:25 +0000 (Sat, 20 Jan 2007) wpijewsk $
 */
public class BDRow implements Serializable {

    private static final long serialVersionUID = -6808858304695434127L;
    protected BDSchema m_schema;
    protected Vector<Object> m_fields;    

    /**
     * Class constructor.
     * 
     * @param data The <code>Map</code> that contains the data used in this
     *            row
     */
    public BDRow(BDSchema data) {
        this.m_schema = data;
    }
    
    /**
     * @param key  The search key 
     * @return The value associated with <code>key</code>
     */
    public Object getField(String key) {
    	return m_fields.elementAt(m_schema.getPosition(key));
    }

    /**
	 * @param position The position of an item in the row
	 * @return The item at that position in this row
	 */
    public Object getField(int position) {
    	return m_fields.elementAt(position);
    }
}