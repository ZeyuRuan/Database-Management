package bdsim.server.system.handler;

import bdsim.server.exec.BDPlan;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTable;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee
 */
public class BDUpdateHandler implements BDHandler {

	private BDPlan m_plan;
	private String m_table;
	
	public BDUpdateHandler(BDPlan p) {
		m_plan = p;
		m_table = p.getTables().get(0);
	}
	
	public BDSystemResultSet execute() throws InterruptedException, RollbackException {
		BDTable tb = BDSystem.tableManager.getTableByName(m_table);
		
		BDHandler handler;
		BDSystemResultSet result;
		
		handler = new BDSelectHandler(m_plan);
		result = handler.execute();
		
		BDTuple newTuple;
		
		for(BDTuple tp : result.getTupleData()) {
			newTuple = new BDTuple(tp.getSchema());
			for(int i = 0; i < tp.getNumCols(); i++) {
				if(m_plan.getData().containsKey(tp.getName(i))) {
					newTuple.setObject(i, m_plan.getData().get(tp.getName(i)));
				} else  {
					newTuple.setObject(i, tp.getField(i));				
				}
			}
			tb.replace(tp, newTuple);
		}
		return result;
	}
}
