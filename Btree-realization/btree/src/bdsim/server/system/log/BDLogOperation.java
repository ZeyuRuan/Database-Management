package bdsim.server.system.log;

import bdsim.server.system.BDTuple;
import bdsim.server.system.log.BDLogTester.BDLogOperationType;

/**
 * An operation on a <code>BDTuple</code> for use in testing the CS 127
 * logging and recovery assignment.
 * 
 * @author wpijewsk
 */
final class BDLogOperation {
	
	private int m_amount;

	private BDTuple m_tuple;

	private BDLogOperationType m_type;

	/**
	 * Class constructor.
	 * @param m_tuple
	 *            The <code>BDTuple</code> in which to perform the operation.
	 * @param m_amount
	 *            The amount of the operation
	 * @param m_type
	 *            The type of operation
	 */
	BDLogOperation(BDTuple m_tuple, int m_amount, BDLogOperationType m_type) {
		this.m_type = m_type;
		this.m_amount = m_amount;
		this.m_tuple = m_tuple;
	}

	/**
	 * @return The amount of the action
	 */
	public int getAmount() {
		return m_amount;
	}

	/**
	 * @return The type of this operation
	 */
	public BDLogOperationType getOpType() {
		return m_type;
	}

	/**
	 * @return The <code>BDTuple</code> on which to perform the action
	 */
	public BDTuple getTuple() {
		return m_tuple;
	}
	
	public String toString() {
		String str = "<" + m_tuple.getObject(BDLogTester.keyFieldName) + ": ";

		if (m_type == BDLogOperationType.ADD && m_amount > 0)
			str += "+" + m_amount;
		else if(m_type == BDLogOperationType.SUBTRACT && m_amount < 0)
			str += "+" + new Integer(m_amount).toString().substring(1);
		else 
			str += "-" + m_amount;
		

		str += ">";
		return str;
	}
}