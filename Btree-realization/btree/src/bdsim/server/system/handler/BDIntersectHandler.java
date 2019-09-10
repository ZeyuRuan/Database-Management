package bdsim.server.system.handler;

import org.apache.log4j.Logger;

import bdsim.server.system.BDSchema;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee
 */
public class BDIntersectHandler implements BDHandler {

	static Logger logger = Logger.getLogger(BDIntersectHandler.class);
	private BDSystemResultSet m_leftSet, m_rightSet;

	public BDIntersectHandler(BDSystemResultSet leftSet,
			BDSystemResultSet rightSet) {
		m_leftSet = leftSet;
		m_rightSet = rightSet;
	}

	public BDSystemResultSet execute() throws RollbackException {
		BDSystemResultSet result = new BDSystemResultSet();
		BDSchema schema = new BDSchema(m_leftSet.getSchema());

		result.setSchema(schema);

		for (BDTuple t1 : m_leftSet.getTupleData()) {
			for (BDTuple t2 : m_rightSet.getTupleData()) {
				if (t1.equals(t2)) {
					result.addRow(t1);
					logger.debug("Intersect: adding new tuple: "
							+ t1.getObjects());
				}
			}
		}
			
		return result;
	}
}
