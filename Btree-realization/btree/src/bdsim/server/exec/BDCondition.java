package bdsim.server.exec;

import bdsim.server.exec.nodes.BDCondOpType;

/**
 * A single condition. Stored in a <code>BDConditionList</code>. Each of the
 * sides can either be a String (for a string literal), a Double (for a number),
 * or a BDTableNamePair(for a column name).
 * 
 * @author wpijewsk
 * @revision $Id: BDCondition.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public final class BDCondition {
	
	public enum ConditionValueType {
		COLUMN, LITERAL, NUMBER;
	}
	
    private BDCondOpType m_condop;
	private Object m_lhs;
	private ConditionValueType m_lhsType;
	private Object m_rhs;
	private ConditionValueType m_rhsType;

	/**
	 * @param rhs
	 * @param condop
	 * @param lhs
	 */
	public BDCondition(Object lhs, BDCondOpType condop, Object rhs) {
		this.m_lhs = lhs;
		this.m_condop = condop;
        this.m_rhs = rhs;   
	}

	public BDCondOpType getCondOp() {
		return m_condop;
	}

	public Object getLhs() {
		return m_lhs;
	}

	public ConditionValueType getLhsType() {
		return m_lhsType;
	}

	public Object getRhs() {
		return m_rhs;
	}

	public ConditionValueType getRhsType() {
		return m_rhsType;
	}	

	public void setCondOp(BDCondOpType condop) {
		this.m_condop = condop;
	}

	public void setLhs(Object lhs, ConditionValueType type) {
		this.m_lhs = lhs;
		this.m_lhsType = type;
	}

	public void setRhs(Object rhs, ConditionValueType type) {
		this.m_rhs = rhs;
		this.m_rhsType = type;
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof BDCondition)) {
			return false;
		}
		BDCondition otherCond = (BDCondition) other;
		boolean condOpsEqual = this.m_condop == otherCond.m_condop;
		boolean valueTypesEqual = this.m_lhsType == otherCond.m_lhsType
				&& this.m_rhsType == otherCond.m_rhsType;
		// m_lhs and m_rhs should not be null
		boolean lhsEqual = this.m_lhs.equals(otherCond.m_lhs);
		boolean rhsEqual = this.m_rhs.equals(otherCond.m_rhs);
		
		return condOpsEqual && valueTypesEqual && rhsEqual && lhsEqual;
	}
	
	public String toString() {
		
		String string = m_lhs.toString();

		string += " " + m_condop + " ";
		
		// TODO Actually do this
		switch(m_condop) {
		
		}
		
		string += m_rhs.toString();
		return string;
	}
}

