package bdsim.server.system.handler;

import java.util.Vector;

import bdsim.server.system.BDSchema;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

public class BDInnerJoinHandler implements BDHandler {

	private BDSystemResultSet m_leftSet, m_rightSet;

	public BDInnerJoinHandler(BDSystemResultSet leftSet,
			BDSystemResultSet rightSet) {
		m_leftSet = leftSet;
		m_rightSet = rightSet;
	}

	public BDSystemResultSet execute() throws RollbackException {
		BDSystemResultSet result = new BDSystemResultSet();
		BDSchema temp = new BDSchema(m_leftSet.getSchema());
		BDSchema temp2 = m_rightSet.getSchema();

		for (int i = 0; i < temp2.size(); i++) {
			temp.add(temp2.getName(i), temp2.getObjectType(i));
		}

		result.setSchema(temp);

		int i;

		for (BDTuple t1 : m_leftSet.getTupleData()) {
			for (BDTuple t2 : m_rightSet.getTupleData()) {
				BDTuple newTuple = new BDTuple(temp);
				for (i = 0; i < t1.getSchema().size(); i++) {
					newTuple.setObject(i, t1.getObject(i));
				}
				for (int j = 0; j < t2.getSchema().size(); j++) {
					newTuple.setObject(i + j, t2.getObject(j));
				}
				result.addRow(newTuple);
			}
		}

		Vector<BDTuple> tuplesToRemove = new Vector<BDTuple>();
		for (BDTuple t : result.getTupleData()) {
			for (Object o : t.getObjects()) {
				if (o == null) {
					if (!tuplesToRemove.contains(t)) {
						tuplesToRemove.add(t);
					}
				}
			}
		}

		for (BDTuple t : tuplesToRemove) {
			result.remove(t);
		}

		return result;
	}

}
