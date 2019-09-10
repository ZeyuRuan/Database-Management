package bdsim.server.system.index;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import bdsim.server.system.BDTuple;


/**
 * @author wpijewsk, ebuzek
 * 
 * CS127 NOTE:
 * The BDBPlusTreeNode is the basic building block of our B+ Tree.
 * 
 * Recall that all nodes of a B+ tree contain search-key values. 
 * Besides that, leaf nodes contain pointers to the actual objects (BDTuple),
 * whereas non-leaf nodes contain pointers to children (BDBPlusTreeNode)
 * We store keys and pointers inside of Vector objects.
 * 
 * There are no leaf-to-leaf pointers as in the B+ Tree described in the textbook.
 * 
 * BDBPlusTreeNode has public methods for finding, inserting and deleting keys/children/pointers.
 * These methods generally return a boolean value to help you with splitting/coalescing of the nodes.
 * 
 * It is essential that you understand the functionality of BDBPlusTreeNode.
 */
@SuppressWarnings("unchecked")
public class BDBPlusTreeNode implements Comparable {
	
	static Logger logger = Logger.getLogger(BDBPlusTreeNode.class);
    
	private int m_d; //the size (d-value) of this node
    private int m_id; //unique ID of this node. May be simply incremental.
    private boolean m_leaf; //flag indicating a leaf node
    
    /**
     * @see java.util.Vector
     * CS127 NOTE: Vector is very similar to ArrayList (it's elements are ordered, accessible by index)
     * however, Vectors are synchronized. Note that you don't need to care about thread safety.
     */
    private Vector<Comparable> m_keys;
    private Vector<BDBPlusTreeNode> m_pointers;
    private Vector<BDTuple> m_tuples;
    
    /**
	 * Creates and initializes a new BDBPlusTreeNode
	 * 
	 * @param d The d-value of the node
	 * @param id Unique id. Mostly for debugging purposes. Simple incrementing counter is enough.
	 * @param leaf True for leaf nodes, false otherwise
	 */
	public BDBPlusTreeNode(int d, int id, boolean leaf)
    {
        m_d = d;
        m_id = id;
        m_leaf = leaf;
        m_pointers = new Vector<BDBPlusTreeNode>();
        m_keys = new Vector<Comparable>(2*m_d);
        m_tuples = new Vector<BDTuple>(2*m_d);
    }
    
	/**
	 * A helper method for wiping the entire node
	 * (think when the B+Tree might want to use this...)
	 */
	public void clear() {
		this.m_keys.clear();
		this.m_pointers.clear();
		this.m_tuples.clear();
	}
    
    /**
     * Inserts a search-key into the node.
     * If the node is full (split is needed), this method returns false.
     * 
     * @param key The key to be inserted into the node
     * @return false when split is needed
     */
    public boolean insertKey(Comparable key) 
    {
        int i;
        if(keyCount() == 2*m_d) return false;

        for(i = 0; i < keyCount(); i++)
        {
            if(getKey(i).compareTo(key) > 0)
            {
                m_keys.insertElementAt(key, i);
                return true;
            }
        }
  
        //val is > everything here
        m_keys.add(key);
        return true;
    }
    
    
    /**
     * Deletes a search-key from a node.
     * 
     * @param key The key to be deleted
     * @return false if a merge is needed
     */
    public boolean deleteKey(Comparable key)  //returns 
    {
        for(int i = 0; i < keyCount(); i++)
        {
            if(getKey(i) == key)
            {
                m_keys.removeElementAt(i);
                break;
            }
        }
        if(keyCount() < m_d) return false;
        else return true;
    }

    
	/**
     * Deletes a child.
     * 
     * @param key - the key associated with the child
     * @param child - the pointer to be removed
     * @return false when a merge is needed
     */
    public boolean deleteChild(Comparable key, BDBPlusTreeNode child)
    {
        int i;
        for(i = 0; i < childCount(); i++)
        {
            if(getChild(i) == child)
            {
                m_pointers.remove(i); 
                break;
            }
        }
        deleteKey(key);
        if(childCount() < (m_d + 1)) return false;
        else return true;
    }

    
    /**
	 * Inserts a child into the node. If the node is full, false is returned.
	 * 
	 * @param key The key associated with the child
	 * @param child The pointer to be inserted into the node
	 * @return false when the node is full
	 */
	public boolean insertChild(Comparable key, BDBPlusTreeNode child)
	{
	    if (childCount() == (2 * m_d) + 1)
			return false;
		// Room here for the child
		int i;
	
		if (keyCount() > 0) {
			if (getKey(keyCount() - 1).compareTo(key) == 0
					&& getChild(childCount() - 1).compareTo(child) < 0) {
				// add last pointer.
				m_pointers.add(child);
				return true;
	
			}
		}
	    
	    
	    if(keyCount() == 0)
	    {   //No values, but we have one child
	        if(childCount() > 0 && getChild(0).getKey(0).compareTo(child.getKey(0)) > 0)
	        {
	            m_keys.add(0, key);
	            m_pointers.add(0, child);
	            return true;
	        }
	        else
	        {
	            m_keys.add(key);
	            m_pointers.add(child);
	            return true;
	        }
	    }
	    for(i = 0; i < keyCount(); i++)
	    {
	        if(getKey(i).compareTo(key) > 0)
	        {
	            m_keys.add(i, key);
	            if(key.compareTo(child.getKey(0)) <= 0)
	                m_pointers.add(i+1, child);
	            else m_pointers.add(i, child);
	            return true;
	        }
	    }
	    
	    //new child only has stuff greater than we've seen
	    m_keys.add(key);
	    m_pointers.add(child);
	    return true;
	}

	/**
     * A helper function for creating a root node with two pointers.
     * @param child1 - pointer1
     * @param child2 - pointer2 
     * @param key - the only key of the new root node
     * @return
     */
    public boolean makeRoot(BDBPlusTreeNode child1, BDBPlusTreeNode child2, Comparable key)
    {
        //int val1;
        Comparable val2;
        //val1 = child1.value(0);
        val2 = child2.getKey(0);
        if(val2.compareTo(key) >= 0)
        {
            m_keys.add(0, key);
            m_pointers.add(0, child2);
            m_pointers.add(0, child1);
        }
        else
        {
            m_keys.add(0, key);
            m_pointers.add(0, child1);
            m_pointers.add(0, child2);
        }
        return true;
    }

    
    /**
     * Replaces a key with another.
     * 
     * @param oldKey - the key you want to replace
     * @param newKey
     */
    public void replace(Comparable oldKey, Comparable newKey)
    {
        int i;
        for(i = 0; i < keyCount(); i++)
        {
            if(getKey(i) == oldKey){
                m_keys.set(i, newKey);
                return;
            }
        }
    }

    /**
     * This unique ID is mostly for debugging
     * (feel free to use it to figure out which node you're dealing with,
     * but it should not play any significant role in your implementation)
     * 
     * @return a unique id for this node
     */
    public int getId()
    {
        return m_id;
    }

	
    /**
     * @return true if this node is a leaf node
     */
    public boolean isLeaf()
    {
        return m_leaf;
    }

	/**
	 * @param index
	 * @return the key stored at this 0-based index
	 */
	public Comparable getKey(int index)
    {
        return m_keys.elementAt(index);
    }
		
	/**
	 * @param i Index of the tuple you want
	 * @return tulpe at index i
	 * @throws InterruptedException
	 * 
	 * CS127 NOTE: Since we don't have to deal with multiple threads,
	 * the locking-related code is commented out
	 */
	public BDTuple getTuple(int i) throws InterruptedException {
//		if (Thread.currentThread() instanceof BDSystemThread) {
//		m_tuples.elementAt(i).getLock().requestShared(
//				((BDSystemThread) Thread.currentThread())
//						.getTransactionId());
//	}
		return m_tuples.elementAt(i);
	}

	/**
	 * @param index of the child node you want
	 * @return the child node at this 0-based index
	 */
	public BDBPlusTreeNode getChild(int index)
    {
        return m_pointers.elementAt(index);
    }
	
	/**
	 * @return All the child nodes of this node
	 */
	public List<BDBPlusTreeNode> getChildren() {
		return m_pointers;
	}

	
	/**
	 *
	 * root: 0 <= keyCount <= 2 * d
	 * non-root: d <= keyCount <= 2 * d
	 * @return the number of keys currently at this node
	 */
	public int keyCount()
    {
        return m_keys.size();
    }

	/**
	 * root: 0 <= childCount <= 2 * d + 1
	 * non-root: d + 1 <= childCount <= 2 * d + 1
	 * @return the number of children of this node
	 */
	public int childCount()
    {
        return m_pointers.size();
    }


	/**
	 * @return the order (size) of this node
	 */
	public int d()
    {
        return m_d;
    }
		
    
	/**
	 * inserts tuple into this node
	 * 
	 * @param key
	 * @param t
	 * @return false if a split is needed
	 * @throws InterruptedException
	 * 
	 * CS127 NOTE: Since we don't have to deal with multiple threads,
	 * the locking-related code is commented out
	 */
	public boolean insertTuple(Comparable key, BDTuple t)
			throws InterruptedException {
//		if (Thread.currentThread() instanceof BDSystemThread) {
//		t.getLock().requestExclusive(
//				((BDSystemThread) Thread.currentThread())
//						.getTransactionId());
//	}
		int i;
		if (keyCount() == 2 * m_d) {
			return false;
		}
        
        logger.debug("Inserted tuple: " + t);
        logger.debug("... first field: " + t.getName(0));
        logger.debug("... first value: " + t.getObject(0));

        for(i = 0; i < keyCount(); i++)
        {
            if(getKey(i).compareTo(key) > 0)
            {
                m_keys.insertElementAt(key, i);
                m_tuples.insertElementAt(t, i);
                return true;
            }
        }
  
        //val is > everything here
        m_keys.add(key);
        m_tuples.add(t);
        return true;
	}

	/**
	 * deletes tuple t from this node
	 * 
	 * @param key The key corresponding to the Tuple to be deleted.
	 * @param t The tuple to be deleted.
	 * @return false if a merge is needed
	 * @throws InterruptedException
	 * 
	 * CS127 NOTE: BDTuple t is useful only in a multi-threaded implementation
	 * You don't have to worry about multiple threads messing with your
	 * tuples, and that is why the locking code is commented out... 
	 */
	public boolean deleteTuple(Comparable key, BDTuple t)
			throws InterruptedException {
//		if (Thread.currentThread() instanceof BDSystemThread) {
//			m_tuples.elementAt(i).getLock().requestShared(
//					((BDSystemThread) Thread.currentThread())
//							.getTransactionId());
//		}
		for (int i = 0; i < keyCount(); i++) {
			if (getKey(i).compareTo(key) == 0) {
				m_keys.removeElementAt(i);
				m_tuples.removeElementAt(i);
				break;
			}
		}
		if (keyCount() < m_d)
			return false;
		else
			return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Object arg0) {
		if(!(arg0 instanceof BDBPlusTreeNode)) {
			throw new ClassCastException("Node comparison failed: passed an object that was not a node.");
		}
		BDBPlusTreeNode that = (BDBPlusTreeNode)arg0;
		
		int i = this.getKey(0).compareTo(that.getKey(0));
		return i;
	}

	/**
	 * Returns whether this contains the given key.
	 * 
	 * 
	 * @param key
	 * @return
	 * @author mme
	 */
	public boolean containsKey(Comparable key) {
		for (int i = 0; i < m_keys.size(); i++) {
			if (m_keys.elementAt(i).compareTo(key) == 0) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Returns the child pointed to by the last non-null pointer in this node.
	 * 
	 * 
	 * @return
	 * @author mme
	 */
	public BDBPlusTreeNode getLastChild() {
		return m_pointers.lastElement(); // TODO ensure this is non-null if it's not already ensured
	}	  
}
