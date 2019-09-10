package bdsim.server.system.concurrency;

/**
 * @author dclee
 */
public enum LockStatus {
	WAIT, ACQUIRED, RELEASED, ABANDONED, FAILED
};
