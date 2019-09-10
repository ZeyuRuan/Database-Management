package bdsim.server.exec;

/**
 * A list of conditions taken from the parsed condition list. Think of this
 * class as a three-tuple (C, A, R), where C is one condition, A is a boolean
 * describing if C is anded together with the rest of the conditions, and R is
 * the list of the rest of the conditions.
 * 
 * @author wpijewsk
 * @revision $Id: BDConditionList.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class BDConditionList {

	private boolean m_andedTogether;
	private BDCondition m_condition;
	private BDConditionList m_rest;

	/**
	 * @param condition
	 * @param together
	 * @param rest
	 */
	public BDConditionList(BDCondition condition, boolean together,
			BDConditionList rest) {
		this.m_condition = condition;
		this.m_andedTogether = together;
		this.m_rest = rest;
	}

	public boolean equals(Object other) {
		if (!(other instanceof BDConditionList)) {
			return false;
		}
		BDConditionList otherCondList = (BDConditionList) other;

		boolean condsEqual = m_condition.equals(otherCondList.m_condition);

		boolean restEqual = false;
		if (this.m_rest == null) {
			restEqual = (otherCondList.m_rest == null);
		} else {
			restEqual = m_rest.equals(otherCondList.m_rest);
		}

		return condsEqual && restEqual
				&& (this.m_andedTogether == otherCondList.m_andedTogether);
	}

	public BDCondition getCondition() {
		return m_condition;
	}

	public int getNumConditions() {
		int num = 0;
		if (m_condition != null) {
			++num;
		}
		if (m_rest != null) {
			num += m_rest.getNumConditions();
		}

		return num;
	}

	public BDConditionList getRest() {
		return m_rest;
	}

	public boolean isAndedTogether() {
		return m_andedTogether;
	}

	public void setAndedTogether(boolean together) {
		m_andedTogether = together;
	}

	public void setCondition(BDCondition m_condition) {
		this.m_condition = m_condition;
	}

	public void setRest(BDConditionList m_rest) {
		this.m_rest = m_rest;
	}
	
	public String toString() {
		String string = "";
		string += m_condition.toString();
		if (m_rest != null) {
			if (m_andedTogether) {
				string += " AND ";
			} else {
				string += " OR ";
			}
		}
		
		return string;
	}
}
