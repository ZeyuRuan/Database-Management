package bdsim.server.system;

import java.io.Serializable;
import java.util.Vector;

/**
 * The schema for use with tuples, tables, sets, etc.
 * 
 * @author dclee
 * @revision $Id: BDSchema.java 293 2007-01-21 16:28:18 +0000 (Sun, 21 Jan 2007) wpijewsk $
 */
public class BDSchema implements Serializable {

	private static final long serialVersionUID = 3544394695099822388L;
	private Vector<String> m_names;
	private Vector<BDObjectType> m_types;	
	
	/**
	 * Copy constructor for the schema.
	 * @param other the schema to be copied
	 */
	public BDSchema(BDSchema other) {
		synchronized(other) {
			m_names = new Vector<String>();
			for(String s : other.getNames()) {
				m_names.add(new String(s));
			}
			
			m_types = new Vector<BDObjectType>();
			for(BDObjectType o : other.getTypes()) {
				m_types.add(o);
			}
		}		
	}

	/**
	 * Main constructor for the schema
	 * @param names the names of each field for the schema
	 * @param types the ObjectTypes of each field for the schema
	 */
	public BDSchema(Vector<String> names, Vector<BDObjectType> types) {
		m_names = names;
		m_types = types;
	}

	/**
	 * Adds another entry at the end of the schema
	 * 
	 * @param col
	 *            the name of the column to be added
	 * @param t
	 *            the type of the newly added column
	 */
	public void add(String col, BDObjectType t) {
		m_names.add(col);
		m_types.add(t);
	}

	public boolean equals(BDSchema s) {
		if (s.size() != this.size())
			return false;
		for (int i = 0; i < this.size(); i++) {
			if (s.getName(i) != this.getName(i))
				return false;
			if (s.getObjectType(i) != this.getObjectType(i))
				return false;
		}
		return true;
	}
	
	/**
	 * Calculates the memory size for this schema. Note that blob support is not
	 * implemented, so the schema must be a definable size.
	 * 
	 * @return
	 */
	public int getMemorySize() {
		// TODO calculate actual memory sizes
		return 0;
	}
	
	/**
	 * Gets the name associate with this position in the schema
	 * @param position the position to get the name for
	 * @return the name for this position
	 */
	public String getName(int position) {
		if(position >= 0 && position < m_names.size())
			return m_names.elementAt(position);
		else return null;
	}
	
	/**
	 * @return All names used in the schema in order
	 */
	public Vector<String> getNames() {
		return m_names;
	}

	/**
	 * Gets the BDObjectType for a given position
	 * 
	 * @param position
	 *            the position to be checked
	 * @return the BDObjectType
	 */
	public BDObjectType getObjectType(int position) {
		if (position >= 0 && position < m_types.size())
			return m_types.elementAt(position);
		else return null;
	}
	
	/**
	 * Gets the position of a given name
	 * 
	 * @param name
	 *            the name of the position to be checked
	 * @return the integer identifier of that position
	 */
	public int getPosition(String name) {
		for (int i = 0; i < m_names.size(); i++) {
			if (name.equals(m_names.elementAt(i)))
				return i;
		}
		return -1;
	}

	/**
	 * @return All the ObjectTypes in the schema in order
	 */
	public Vector<BDObjectType> getTypes() {
		return m_types;
	}

	/**
	 * @param s
	 *            The string containing the name to be checked
	 * @return true if s is the name of a field in this schema
	 */
	public boolean isField(String s) {
		return m_names.contains(s);
	}

	/**
	 * Transforms this schema into a new schema with the attributes of the
	 * schema in the parameters
	 * 
	 * @param that
	 *            the BDSchema on the right hand side of the join
	 * @param joinAttributeName
	 *            the name to be exempted from the schema
	 */
	public void joinWith(BDSchema that, String joinAttributeName) {
		for (int i = 0; i < that.size(); i++) {
			if(that.getName(i) != joinAttributeName) {
				m_names.add(that.getName(i));
				m_types.add(that.getObjectType(i));
			}
		}
		//TODO: calculate new memory size
	}
	
	/**
	 * Removes a field from the schema.
	 * @param position the integer identifier of the field to be removed 
	 */
	public void projectOut(int position) {
		m_names.remove(position);
		m_types.remove(position);
	}
	
	/**
	 * Removes a field from the schema.
	 * 
	 * @param name
	 *            The name of the field to be removed
	 */
	public void projectOut(String name) {
		int position = m_names.indexOf(name);
		m_names.remove(position);
		m_types.remove(position);
		// TODO: calculate new memory size
	}

	/**
	 * @return Number of columns in the schema
	 */
	public int size() {
		return m_names.size();
	}

}
