package bdsim.server.exec;

import bdsim.server.exec.BDSemanticVisitor.BDNameMappings;
import bdsim.server.exec.nodes.NodeStatement;

/**
 * Takes a parsed SQL expression and converts that intermediate representation
 * to a a BDPlan, then optimizes the algorithms of that plan.
 * 
 * @author wpijewsk
 * @revision $Id: BDPlanner.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May
 *           2006) wpijewsk $
 */
public final class BDPlanner {
		
	private NodeStatement m_statement;

	/**
	 * Class constructor.
	 * 
	 * @param statement The parsed SQL expression for which we are generating a
	 * plan.
	 */
	public BDPlanner(NodeStatement statement) {
		this.m_statement = statement;
	}
	
	/**
	 * Forumlates a plan from a parsed SQL expression.
	 * @param nameMappings
	 * 
	 * @return A <code>BDPlan</code> to execute.
	 */
	public BDPlan makePlan(BDNameMappings nameMappings) {
		BDPlan plan = new BDPlan(m_statement, nameMappings);
		plan.generatePlan();
		
		return plan;
	}
	
	/**
	 * Optimizes a plan that has already been generated.
	 * 
	 * @param plan The plan to optimize
	 */
	public void optimizePlan(BDPlan plan) {
		
		// TODO BDPlanner.optimizePlan
	}
}
