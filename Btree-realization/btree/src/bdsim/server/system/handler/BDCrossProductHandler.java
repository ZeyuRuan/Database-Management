package bdsim.server.system.handler;

import org.apache.log4j.Logger;

import bdsim.server.system.BDSchema;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee
 */
public class BDCrossProductHandler implements BDHandler {
	
	static Logger logger = Logger.getLogger(BDCrossProductHandler.class);
	private BDSystemResultSet m_leftSet, m_rightSet;
		
	public BDCrossProductHandler(BDSystemResultSet leftSet, BDSystemResultSet rightSet) {
		m_leftSet = leftSet;
		m_rightSet = rightSet;		
	}
	
	public BDSystemResultSet execute() throws RollbackException {
		BDSystemResultSet result = new BDSystemResultSet();		
		
		BDSchema temp = new BDSchema(m_leftSet.getSchema());
		BDSchema temp2 = m_rightSet.getSchema();		
			
		for(int i = 0; i < temp2.size(); i++) {
			temp.add(temp2.getName(i), temp2.getObjectType(i));
		}		
		
		result.setSchema(temp);
		
		logger.debug("New schema: " + temp.getNames());
		
		int i;
		
		for(BDTuple t1 : m_leftSet.getTupleData()) {
			for(BDTuple t2 : m_rightSet.getTupleData()) {				
				BDTuple newTuple = new BDTuple(temp);					
				for(i = 0; i < t1.getSchema().size(); i++) {
					//System.out.println("Crossing: " + t1.getObject(i));
					newTuple.setObject(i, t1.getObject(i));
				}
				logger.debug("T2 Schema size: " + t2.getSchema().size());
				for(int j = 0; j < t2.getSchema().size(); j++) {
					//System.out.println("Crossing: " + t2.getObject(j));
					newTuple.setObject(i + j, t2.getObject(j));
				}
				result.addRow(newTuple);
			}
		}
			
		return result;
	}
}
