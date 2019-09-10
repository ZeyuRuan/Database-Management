package bdsim.server.system.concurrency;

import java.util.LinkedList;

import bdsim.server.exec.BDPlan;

/**
 * A list of BDPlans which constitute one transaction.
 * 
 * @author wpijewsk
 * @revision $Id: BDTransaction.java 210 2007-01-17 16:09:25 +0000 (Wed, 17 Jan 2007) wpijewsk $
 */
public final class BDTransaction {

	private LinkedList<BDPlan> m_plans;
	private int m_id;

	/**
	 * Class constructor.
	 */
	public BDTransaction() {
		this.m_plans = new LinkedList<BDPlan>();
	}

	public LinkedList<BDPlan> getPlans() {
		return m_plans;
	}

	public void addPlan(BDPlan plan) {
		m_plans.addLast(plan);
	}

	public int getId() {
		return m_id;
	}

	public void setId(int counter) {
		m_id = counter;
	}
}
