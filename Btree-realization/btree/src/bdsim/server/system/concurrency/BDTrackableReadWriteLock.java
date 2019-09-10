package bdsim.server.system.concurrency;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import bdsim.server.system.BDSystemThread;
import bdsim.server.system.BDThreadStatus;

/**
 * A reader-writer lock which allows access to its wait queue so you can,
 * for example, run cycle-breaking on it.
 * 
 * @author acath, wpijewsk
 */
public class BDTrackableReadWriteLock<T extends BDSystemThread> {

	private static Logger logger = Logger.getLogger(BDTrackableReadWriteLock.class);
	
	/**
	 * A group of threads which can run concurrently while holding this lock
	 * (typically, multiple readers or a single writer).
	 *  
	 * @author acath, wpijewsk
	 */
	public static class ConcurrentThreadGroup<E extends Thread> {
		private Set<E> m_threads;

		private boolean m_isExclusive;

		public ConcurrentThreadGroup(boolean isExclusive) {
			m_threads = new HashSet<E>();
			m_isExclusive = isExclusive;
		}

		public void addThread(E t) {
			m_threads.add(t);
		}

		public boolean removeThread(E t) {
			return m_threads.remove(t);
		}

		public Set<E> getThreads() {
			return m_threads;
		}

		public boolean isExclusive() {
			return m_isExclusive;
		}
	}

	private LinkedList<ConcurrentThreadGroup<T>> m_threadGroups;
	private String m_name;

	public BDTrackableReadWriteLock(String name) {
		m_threadGroups = new LinkedList<ConcurrentThreadGroup<T>>();
		m_name = name;
	}

	public List<ConcurrentThreadGroup<T>> getThreadGroups() {
		return m_threadGroups;
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void abandon() {	
		// remove ourselves from any groups to which we belong, and delete
		// any now-empty groups
		for (ConcurrentThreadGroup<T> group : m_threadGroups) {
			if (group.removeThread((T)Thread.currentThread()) &&
					group.getThreads().size() == 0) {
				m_threadGroups.remove(group);
			}
		}
		// if we held the lock, we now need to notify the first group. No harm
		// in doing it regardless, though, since wait()s are in while loops.
		if (m_threadGroups.size() > 0) {
			this.notifyAll();
		}
	}
	
	public synchronized void readLock() throws RollbackException {
		// if unlocked or last thread group is exclusive, create a new group
		logger.debug(Thread.currentThread() + " requesting read lock on  " + this);
		if (m_threadGroups.isEmpty() || m_threadGroups.getLast().isExclusive()) {
			m_threadGroups.addLast(new ConcurrentThreadGroup<T>(false));
		}
		addToLastGroupAndWait();
		logger.debug(Thread.currentThread() + " got read lock on         " + this);
	}

	public synchronized void writeLock() throws RollbackException {
		// always create a new group
		logger.debug(Thread.currentThread() + " requesting write lock on " + this);
		m_threadGroups.addLast(new ConcurrentThreadGroup<T>(true));
		addToLastGroupAndWait();
		logger.debug(Thread.currentThread() + " got write lock on        " + this);
	}

	@SuppressWarnings("unchecked")
	public synchronized void unlock() {
		// remove ourselves from the our thread group, which must be at the
		// front of the list (otherwise we couldn't be running)
		ConcurrentThreadGroup<T> myGroup = m_threadGroups.getFirst();
		myGroup.removeThread((T)Thread.currentThread());
		if (myGroup.getThreads().size() == 0) {
			m_threadGroups.removeFirst();
			if (m_threadGroups.size() > 0) {
				this.notifyAll();
			}
		}		
		logger.debug(Thread.currentThread() + " unlocked                 " + this);
	}
	
	@Override
	public String toString() {
		return "BDTrackableReadWriteLock[name=" + m_name + "]";
	}
	
	@SuppressWarnings("unchecked")
	private synchronized void addToLastGroupAndWait() throws RollbackException {
		
		// add thread to the last group
		ConcurrentThreadGroup<T> myGroup = m_threadGroups.getLast();
		myGroup.addThread((T)Thread.currentThread());
		
//      if (myGroup != m_threadGroups.getFirst()) {
//         System.err.print(Thread.currentThread() + ": Waiting for myGroup[");
//         for (T thread : myGroup.getThreads()) {
//            System.err.print(thread.getTransactionId() + ", ");
//         }
//         System.err.print("], saw Group[");
//         for (T thread : m_threadGroups.getFirst().getThreads()) {
//            System.err.print(thread.getTransactionId() + ", ");
//         }
//         System.err.println("]");
//      } else {
//         System.err.println("PAVLO: Not waiting!!");
//      }

      // then wait until we're at the front
		while (m_threadGroups.getFirst() != myGroup) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				if (((T) Thread.currentThread()).getThreadStatus() == BDThreadStatus.ROLLING_BACK) {
					throw new RollbackException();
				}
			}
		}
		
		assert m_threadGroups.getFirst() == myGroup;
	}

}