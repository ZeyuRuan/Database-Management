package bdsim.server.system;

/**
 * This class manages the simulated memory system of the database
 * Memory is allocated/freed by external classes  by the various request/release calls
 * 
 * @author dclee
 */

public class BDMemoryManager {

	private int m_pageSize, m_availableBlocks;
	
	/**
	 * Creates a new memory manager for this system.
	 * Intended to be statically initialized in BDSystem.
	 * 
	 * @param pageSize the intended size of a block in bytes
	 * @param numPages the number of blocks that can be held by the system
	 */
	public BDMemoryManager(int pageSize, int numPages) {
		m_pageSize = pageSize;
		m_availableBlocks = numPages;
	}
	
	/**
	 * Requests memory in bytes. Useful if only part of a tuple must be allocated
	 * @param bytes the number of bytes to be allocated
	 * @return true if able to allocate the given amount, false if not
	 */
	public boolean requestMemory(int bytes) {
		if(bytes <= m_pageSize * m_availableBlocks) {
			m_availableBlocks -= (bytes / m_pageSize);
			return true;
		}
		else return false;
	}
	
	/**
	 * Requests memory for a tuple. Preferred to requesting in bytes if tuple schema is known
	 * @param t the tuple (and more importantly its schema) to be allocated for
	 * @param numOfTs number of tuples of this schema to allocated memory for
	 * @return
	 */
	public boolean requestMemory(BDTuple t, int numOfTs) {
		int bytes = t.getSchema().getMemorySize() * numOfTs;
		
		if(bytes <= m_pageSize * m_availableBlocks) {
			m_availableBlocks -= (bytes / m_pageSize);
			return true;
		}
		else return false;
	}
	
	/**
	 * Releases memory for a single tuple. Assumes this tuple stands on its own and is taking up a block;
	 * This method is more for support for smaller operations, release as a BDResultSet whenever possible.
	 * @param t
	 */
	public void releaseMemory(BDTuple t) {
		m_availableBlocks++;
	}	
	
	/**
	 * Releases memory for an entire result set. Preferred to releasing single tuples
	 * @param rs the BDSystemResultSet to be released
	 */
	public void releaseMemory(BDSystemResultSet rs) {
		m_availableBlocks += (rs.getMemorySize() / m_pageSize);
	}


	public void requestMemory(BDSystemResultSet result) {
		m_availableBlocks -= (result.getMemorySize() / m_pageSize);
	}
}
