package bdsim.server.system.handler;

import java.util.List;

import org.apache.log4j.Logger;

import bdsim.server.exec.BDOrderByInfo;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee
 */
public class BDOrderByHandler implements BDHandler {

	private BDSystemResultSet m_original;
	private String m_table;
	private List<BDOrderByInfo> m_ordering;
	
	private static Logger logger = Logger.getLogger(BDOrderByHandler.class);
	
	public BDOrderByHandler(BDSystemResultSet original, List<BDOrderByInfo> columnsToProject) {
		m_table = null;
		m_original = original;
		m_ordering = columnsToProject;
	}

	public BDSystemResultSet execute() throws InterruptedException, RollbackException {

		logger.debug("Ordering by: "
				+ m_ordering.get(0).getColumn().getColumn());

		BDSystem.memoryManager.requestMemory(m_original);

		if (m_original == null) {
			//Must get data from table first

			//Need manager for all system name resources!!!
			m_original = BDSystem.tableManager.getTableByName(m_table)
					.getAllTuples();
		}

		for (BDTuple t : m_original.getTupleData()) {
			t.setTempKey(m_ordering.get(0).getColumn().getColumn());
		}

		m_original.sort(m_ordering.get(0).isAscending());

		BDSystem.memoryManager.releaseMemory(m_original);

		return m_original;
	}
}
