package bdsim.server.exec;

/**
 * Uniquely identifies a column by specifying a table and a field name.
 * 
 * @author dclee
 * @revision $Id: BDTableColumnPair.java 161 2006-05-08 19:44:39 +0000 (Mon, 08 May 2006) wpijewsk $
 */
public class BDTableColumnPair {
	
	private String m_table;
	private String m_column;

	public BDTableColumnPair(String table, String column) {
		m_table = table;
		m_column = column;
	}

	public String getTable() {
		return m_table;
	}

	public String getColumn() {
		return m_column;
	}
	
    @Override
    public boolean equals(Object other) {
        if(other == null || !(other instanceof BDTableColumnPair)) {
            return false;
        } else {
        	BDTableColumnPair otherPair = (BDTableColumnPair) other;
			boolean tableEqual = this.m_table.equalsIgnoreCase(otherPair.m_table);
			boolean colEqual = this.m_column.equalsIgnoreCase(otherPair.m_column);
        	return tableEqual && colEqual;
		}
    }
    
    @Override
    public String toString() {
    	return m_table + "." + m_column;
    }
    
    public int hashCode() {
    	return m_table.hashCode() + m_column.hashCode();
    }
}