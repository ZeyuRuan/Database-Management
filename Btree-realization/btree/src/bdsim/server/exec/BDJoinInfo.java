package bdsim.server.exec;

import bdsim.server.exec.nodes.BDJoinType;

/**
 * Holds information about the joins in a plan.
 * 
 * @author wpijewsk
 */
public class BDJoinInfo {

	private BDTableColumnPair m_table1;
	private BDTableColumnPair m_table2;
	private BDJoinType m_joinType;
	private BDJoinAlgo m_algo;

	/**
	 * Class constructor.
	 * 
	 * @param table1
	 *            The left hand table to join
	 * @param table2
	 *            The right hand table to join
	 * @param type
	 *            The type of join
	 * @param algo
	 *            The algorithm to use
	 */
	public BDJoinInfo(BDTableColumnPair table1, BDTableColumnPair table2,
			BDJoinType type, BDJoinAlgo algo) {
		this.m_table1 = table1;
		this.m_table2 = table2;
		this.m_joinType = type;
		this.m_algo = algo;
	}

	public boolean equals(Object other) {
		if (!(other instanceof BDJoinInfo)) {
			return false;
		}
		BDJoinInfo otherInfo = (BDJoinInfo) other;

		return m_table1.equals(otherInfo.m_table1)
				&& m_table2.equals(otherInfo.m_table2)
				&& m_joinType == otherInfo.m_joinType
				&& m_algo == otherInfo.m_algo;
	}

	public BDJoinAlgo getAlgo() {
		return m_algo;
	}

	public BDJoinType getJoinType() {
		return m_joinType;
	}

	public BDTableColumnPair getTable1() {
		return m_table1;
	}

	public BDTableColumnPair getTable2() {
		return m_table2;
	}
}