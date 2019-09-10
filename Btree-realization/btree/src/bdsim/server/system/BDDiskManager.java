package bdsim.server.system;

import java.util.HashMap;
import java.util.Map;


/**
 * @author dclee, wpijewsk
 */
public class BDDiskManager {
	
	private Map<Integer,Integer> m_transactionIOs;

	public BDDiskManager() {
		m_transactionIOs = new HashMap<Integer,Integer>();
	}
	
	public int getTransactionIOs(int TID) {
		return m_transactionIOs.get(TID);
	}
	
	public void addIOs(int TID, int ios) {
		if(m_transactionIOs.get(TID) == null)
			m_transactionIOs.put(TID, 0);
		m_transactionIOs.put(TID, m_transactionIOs.get(TID) + ios);
	}
	
	// TODO Something with concurrency controller
	
}
