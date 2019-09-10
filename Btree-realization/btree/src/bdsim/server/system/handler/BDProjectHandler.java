package bdsim.server.system.handler;

import java.util.LinkedList;
import java.util.List;

import bdsim.server.system.BDSchema;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee
 */
public class BDProjectHandler implements BDHandler {
	
	private BDSystemResultSet m_original;
	private String m_table;
	private List<String> m_columnsToProject;
	
	//TODO add a reference to the Thread itself so that it can yield properly
	public BDProjectHandler(BDSystemResultSet original, List<String> columnsToProject) {
		m_table = null;
		m_original = original;
		m_columnsToProject = columnsToProject;
	}

	public BDSystemResultSet execute() throws InterruptedException, RollbackException {
	
		if(m_original ==  null) {
			//Must get data from table first
			
			//Need manager for all system name resources!!!
			m_original = BDSystem.tableManager.getTableByName(m_table).getAllTuples();			
			//TODO must gaurantee that this is a COPY
			
			//Yield control over to another thread
		}
		
		BDSystemResultSet result = new BDSystemResultSet();		
									
		BDSchema schema = new BDSchema(m_original.getSchema());
		
		List<String> stuffToProjectOut = new LinkedList<String>();
		
		for(String col : schema.getNames()) {
			if(!m_columnsToProject.contains(col))
				stuffToProjectOut.add(col);
		}
		
		for(String col : stuffToProjectOut) {
			schema.projectOut(col);
		}
		
		result.setSchema(schema);
		
		BDSystem.memoryManager.requestMemory(schema.getMemorySize()*m_original.getNumTuples());		
		
		String name;
		
		for(BDTuple t : m_original.getTupleData()) {
			BDTuple newTuple = new BDTuple(schema);
			int copyCounter = 0;
			for(int i = 0; i < t.getNumCols(); i++) {
				name = schema.getName(copyCounter);
				if((t.getName(i)).equals(name)) {
					newTuple.setObject(copyCounter, t.getObject(i));
					copyCounter++;
				}
			}
			result.addRow(newTuple);
		}
		
		BDSystem.memoryManager.releaseMemory(m_original);
		
		return result;
	}

}
