package bdsim.server.exec;

/**
 * Holds information about an order by entry: which field to order by and in
 * what direction.
 * 
 * @author wpijewsk
 * @revision $Id: BDOrderByInfo.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class BDOrderByInfo {

	private BDTableColumnPair m_column;
	private boolean m_ascending;

	public BDOrderByInfo(BDTableColumnPair column, boolean ascending) {
		this.m_column = column;
		this.m_ascending = ascending;
	}

	public boolean isAscending() {
		return m_ascending;
	}

	public BDTableColumnPair getColumn() {
		return m_column;
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof BDOrderByInfo)) {
			return false;
		}
		BDOrderByInfo otherInfo = (BDOrderByInfo) other;

		return this.m_column.equals(otherInfo.m_column)
				&& this.m_ascending == otherInfo.m_ascending;
	}
}
