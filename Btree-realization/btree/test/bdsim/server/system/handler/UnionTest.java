package bdsim.server.system.handler;

import java.util.Vector;

import junit.framework.TestCase;
import bdsim.server.system.BDObjectType;
import bdsim.server.system.BDSchema;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDSystemThread;
import bdsim.server.system.BDTuple;
import bdsim.server.system.concurrency.RollbackException;

public class UnionTest extends TestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}

	public void testSelectEqual() throws RollbackException {
		BDSystemThread sysThread = new BDSystemThread(0) {
			public void run() {
				try {
					BDSystemResultSet lhs = new BDSystemResultSet();
					BDSystemResultSet rhs = new BDSystemResultSet();

					Vector<String> names1 = new Vector<String>();
					names1.addElement("Num1");
					names1.addElement("Num2");

					Vector<BDObjectType> types1 = new Vector<BDObjectType>();
					types1.addElement(BDObjectType.INTEGER);
					types1.addElement(BDObjectType.INTEGER);

					BDSchema schema1 = new BDSchema(names1, types1);

					BDTuple tup1 = new BDTuple(schema1);
					tup1.setObject("Num1", new Double(3));
					tup1.setObject("Num2", new Double(5));
					lhs.addRow(tup1);

					BDTuple tup2 = new BDTuple(schema1);
					tup2.setObject("Num1", new Double(7));
					tup2.setObject("Num2", new Double(9));
					lhs.addRow(tup2);

					// TEST TABLE TWO
					// Name: Test2
					// HAS 2 COLUMNS, BOTH WITH INT VALUES NAMED Num1 and Num2, num1 is
					// primary key

					BDTuple tup3 = new BDTuple(schema1);
					tup3.setObject("Num1", new Double(3));
					tup3.setObject("Num2", new Double(5));
					rhs.addRow(tup3);

					BDTuple tup4 = new BDTuple(schema1);
					tup4.setObject("Num1", new Double(9));
					tup4.setObject("Num2", new Double(3));
					rhs.addRow(tup4);

					BDHandler h = new BDUnionHandler(lhs, rhs);
					BDSystemResultSet result = null;
					try {
						result = h.execute();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					assertTrue(result.getNumTuples() == 3);
					assertTrue(result.hasTuple(tup1));
					assertTrue(result.hasTuple(tup2));
					assertTrue(result.hasTuple(tup4));
				} catch (RollbackException e) {
					e.printStackTrace();
					fail("Transaction was unexpectedly rolled back");
				}
			}
		};

		sysThread.start();

		try {
			sysThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail("testSelectEqual was interrupted");
		}
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
}