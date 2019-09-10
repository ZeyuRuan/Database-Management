package bdsim.server.system.handler;

import org.apache.log4j.Logger;

import bdsim.server.exec.BDPlan;
import bdsim.server.exec.BDTableColumnPair;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTable;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * Handles the insertion of data into a specific table.
 * 
 * @author dclee, wpijewsk
 */
public class BDInsertHandler implements BDHandler {

	static Logger logger = Logger.getLogger(BDInsertHandler.class);	
	private BDPlan m_plan;
	private String m_table;
	
	/**
	 * Class constructor.
	 * 
	 * @param plan
	 *            The plan that's being executed
	 */
	public BDInsertHandler(BDPlan plan) {
		m_plan = plan;
		m_table = plan.getTables().get(0);
	}
	
	public BDSystemResultSet execute() throws InterruptedException,
			RollbackException {
		BDTable tb = BDSystem.tableManager.getTableByName(m_table);
		BDSystemResultSet result = new BDSystemResultSet();
		BDTuple tp = new BDTuple(tb.getSchema());
		for (int i = 0; i < tp.getNumCols(); i++) {
			tp.setObject(i, m_plan.getData().get(
					new BDTableColumnPair(m_table, tp.getName(i))));
			logger.debug("New BDTuple: " + i + " "
					+ new BDTableColumnPair(m_table, tp.getName(i)) + " : "
					+ tp.getField(i));
		}
		tb.insert(tp);
		result.addRowWithoutCopy(tp);
		return result;
	}
}