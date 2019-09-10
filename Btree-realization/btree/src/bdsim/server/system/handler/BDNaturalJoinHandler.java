package bdsim.server.system.handler;

import org.apache.log4j.Logger;

import bdsim.server.system.BDSchema;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

public class BDNaturalJoinHandler implements BDHandler {

	private BDSystemResultSet m_leftSet, m_rightSet;
	
	private String m_leftAttrib, m_rightAttrib;
	
	private Logger logger = Logger.getLogger(BDNaturalJoinHandler.class);
	
	public BDNaturalJoinHandler(BDSystemResultSet leftSet, BDSystemResultSet rightSet,
			String leftAttrib, String rightAttrib) {
		m_leftSet = leftSet;
		m_rightSet = rightSet;
		
		m_leftAttrib = leftAttrib;
		m_rightAttrib = rightAttrib;
	}
	
	public BDSystemResultSet execute() throws InterruptedException, RollbackException {
		BDSystemResultSet result = new BDSystemResultSet();		
		
		BDSchema temp = new BDSchema(m_leftSet.getSchema());
		BDSchema temp2 = m_rightSet.getSchema();
		
		for(int i = 0; i < temp2.size(); i++) {			
			if(temp2.getName(i) != null && !m_rightAttrib.equals(temp2.getName(i)))
				temp.add(temp2.getName(i), temp2.getObjectType(i));
		}		
		
		result.setSchema(temp);		
		
		logger.debug("New schema: " + temp.getNames());				
		
		String name;
		int i, copyCounter;
		
		for(BDTuple t1 : m_leftSet.getTupleData()) {
			for(BDTuple t2 : m_rightSet.getTupleData()) {
				if(t1.getField(m_leftAttrib).equals(t2.getField(m_rightAttrib))) {
					BDTuple newTuple = new BDTuple(temp);
					copyCounter = 0;
					for(i = 0; i < t1.getSchema().size(); i++) {
						newTuple.setObject(i, t1.getObject(i));
					}					
					//System.out.println("i = " + i);
					for(int j = 0; j < t2.getSchema().size(); j++) {						
						name = temp.getName(i + copyCounter);
						//System.out.println("schemaname " + name);
						//System.out.println("schemaname " + t2.getName(j));
						if((t2.getName(j)).equals(name)) {
							//System.out.println("copy = " + copyCounter);
							newTuple.setObject(i + copyCounter, t2.getObject(j));
							copyCounter++;
						}
					}
					result.addRow(newTuple);
				}
			}
		}
		BDSystem.memoryManager.requestMemory(result);
		
		BDSystem.memoryManager.releaseMemory(m_leftSet);
		BDSystem.memoryManager.releaseMemory(m_rightSet);
		return result;
	}
}
