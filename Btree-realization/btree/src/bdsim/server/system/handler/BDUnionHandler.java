package bdsim.server.system.handler;

import bdsim.server.system.BDSchema;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee, wpijewsk, acath
 */
public class BDUnionHandler implements BDHandler {

	private BDSystemResultSet m_leftSet, m_rightSet;

	public BDUnionHandler(BDSystemResultSet leftSet, BDSystemResultSet rightSet) {
		m_leftSet = leftSet;
		m_rightSet = rightSet;
	}

	public BDSystemResultSet execute() throws RollbackException {
		BDSystemResultSet result = new BDSystemResultSet();
		result.setSchema(new BDSchema(m_leftSet.getSchema()));

		for (BDTuple t1 : m_leftSet.getTupleData()) {
			result.addRow(t1);
		}

		for (BDTuple t2 : m_rightSet.getTupleData()) {
			if (!(result.hasTuple(t2))) {
				result.addRow(t2);
			}
		}

		BDSystem.memoryManager.requestMemory(result);
		return result;
	}
}
