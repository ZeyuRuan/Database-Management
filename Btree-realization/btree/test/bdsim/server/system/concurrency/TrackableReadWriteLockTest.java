package bdsim.server.system.concurrency;

import junit.framework.TestCase;
import bdsim.server.system.BDSystemThread;

public class TrackableReadWriteLockTest extends TestCase {

	
	@Override
	public void setUp() {
	}
	
	public void testBasicNonInterleaved() throws RollbackException {
		new BDSystemThread(1) {
			public void run() {
				BDTrackableReadWriteLock<BDSystemThread> lock = 
					new BDTrackableReadWriteLock<BDSystemThread>("basicNonInterleaved");
				try {
					lock.readLock();
					lock.unlock();
					lock.writeLock();
					lock.unlock();	
				} catch (RollbackException e) {
					fail();
				}
			}
		}.start();
	}
	
	public void testBasicInterleaved() throws InterruptedException {
		class MyThread extends BDSystemThread {
			BDTrackableReadWriteLock<BDSystemThread> m_lock;
			public MyThread(int tid, BDTrackableReadWriteLock<BDSystemThread> lock) {
				super(tid);
				m_lock = lock;
			}
			public void run() {
				try {
					for (int i = 0; i < 100; i++) {
						m_lock.writeLock();
						yield();
						m_lock.unlock();
					}
				} catch (RollbackException e) {
					fail();
				}
				
			}
		}
		BDTrackableReadWriteLock<BDSystemThread> lock = 
			new BDTrackableReadWriteLock<BDSystemThread>("basicInterleaved");
		Thread t1 = new MyThread(10, lock);
		Thread t2 = new MyThread(20, lock);
		t1.start();
		t2.start();
		t1.join();
		t2.join();
	}
	
	public void testHeavyInterleaved() {
		
	}
	/*
	public void testAbandon() throws RollbackException, InterruptedException {
		final BDTrackableReadWriteLock<BDSystemThread> lock = 
			new BDTrackableReadWriteLock<BDSystemThread>();
		lock.readLock();
		lock.abandon();
		lock.writeLock();
		lock.abandon();
		lock.writeLock();
		lock.unlock();
				
		BDSystemThread first = new BDSystemThread(10) {
			public void run() {
				try {
					lock.writeLock();
				} catch (RollbackException e) {
					fail();
				}
			}
		};
		first.start();
		first.join();
		
		BDSystemThread second = new BDSystemThread(11) {
			public void run() {
				try {
					lock.readLock();
				} catch (RollbackException e) {
					lock.abandon();
				}
			}
		};
		second.start();
		second.join();
	}
	*/	
}
