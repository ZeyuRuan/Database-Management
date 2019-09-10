package bdsim.server.system.log;

import java.util.List;

/**
 * A collection of <code>BDLogOperation</code>s to execute as a single
 * transaction. All of these operations will not affect the total sum of values
 * in the transaction, but will leave inconsistent states in between.
 * 
 * @author wpijewsk
 */
final class BDLogTransaction {

	private List<BDLogOperation> m_operations;

	/**
	 * Class constructor.
	 * 
	 * @param m_operations
	 *            The list of operations in this transaction
	 */
	BDLogTransaction(List<BDLogOperation> m_operations) {
		this.m_operations = m_operations;
	}

	public String toString() {
		String str = "(";
		for (BDLogOperation operation : m_operations) {
			str += operation.toString() + ", ";
		}
		str = str.substring(0, str.length() - 2) + ")";
		return str;
	}

	/**
	 * @return The list of operations in this transaction
	 */
	List<BDLogOperation> getOperations() {
		return m_operations;
	}
}
