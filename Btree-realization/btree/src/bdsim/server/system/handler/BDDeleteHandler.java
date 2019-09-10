package bdsim.server.system.handler;

import org.apache.log4j.Logger;

import bdsim.server.exec.BDPlan;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTable;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee, wpijewsk
 */
public class BDDeleteHandler implements BDHandler {

	static Logger logger = Logger.getLogger(BDDeleteHandler.class);
	private BDPlan m_plan;
	private String m_table;

	public BDDeleteHandler(BDPlan p) {
		m_plan = p;
		m_table = p.getTables().get(0);
	}

	public BDSystemResultSet execute() throws InterruptedException,
			RollbackException {
		BDTable tb = BDSystem.tableManager.getTableByName(m_table);
		
		BDHandler handler;
		BDSystemResultSet result;
		
		handler = new BDSelectHandler(m_plan);
		result = handler.execute();
		
		for(BDTuple tp : result.getTupleData()) {
			logger.debug("Deleting: " + tp);
			tb.delete(tp);
		}
		return result;
	}

}