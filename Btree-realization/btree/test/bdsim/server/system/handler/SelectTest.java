package bdsim.server.system.handler;

import java.util.List;

import junit.framework.TestCase;
import bdsim.common.BDResultSet;
import bdsim.server.exec.BDParseException;
import bdsim.server.exec.BDPlan;
import bdsim.server.exec.BDPlanner;
import bdsim.server.exec.BDSemanticVisitor;
import bdsim.server.exec.BDSqlParser;
import bdsim.server.exec.BDSemanticVisitor.BDNameMappings;
import bdsim.server.exec.BDSemanticVisitor.SemanticException;
import bdsim.server.exec.nodes.NodeStatement;
import bdsim.server.system.BDSystem;
import bdsim.server.system.concurrency.BDTransaction;
import bdsim.server.system.concurrency.RollbackException;

/**
 * Tests selection commands.
 * 
 * @author wpijewsk
 */
public class SelectTest extends TestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		BDSystem.tableManager.readFromXmlFile("test/files/Bank.xml");
	}

	private BDResultSet runSql(String sql) throws BDParseException,
			SemanticException, InterruptedException {
		BDTransaction trans = new BDTransaction();
		int TID = 0;

		// Parse SQL
		BDSqlParser parser = new BDSqlParser();
		List<NodeStatement> stmtList = parser.parse(sql);
		BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
		for (NodeStatement stmt : stmtList) {
			stmt.visit(smtcVisitor);

			if (smtcVisitor.resolveNames()) {
				// Process parsed statement
				BDNameMappings mappings = smtcVisitor.getMappings();
				BDPlanner planner = new BDPlanner(stmt);
				BDPlan plan = planner.makePlan(mappings);
				trans.addPlan(plan);
			}
		}

		TID = BDSystem.scheduler.addTransaction(trans);
		trans.setId(TID);

		if (TID != -1) {
			// Blocking call to wait for response

			while (!BDSystem.scheduler.isFinished(TID)) {
				Thread.sleep(500);
			}
			return BDSystem.scheduler.getResult(TID);
		}
		return null;
	}
	
	public void testSelectOfAccountsWholeTable() throws RollbackException {
		String sql = "SELECT * from Accounts";
		try {
			BDResultSet results = runSql(sql);
			assertEquals(17, results.getData().size());
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("BDParseException: " + e.getMessage());
		} catch (SemanticException e) {
			e.printStackTrace();
			fail("SemanticException: " + e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail("InterruptedException: " + e.getMessage());
		}
	}
	
	public void testSelectOfBranchesWholeTable() throws RollbackException {
		String sql = "SELECT * from Branches";
		try {
			BDResultSet results = runSql(sql);
			assertEquals(15, results.getData().size());
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("BDParseException: " + e.getMessage());
		} catch (SemanticException e) {
			e.printStackTrace();
			fail("SemanticException: " + e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail("InterruptedException: " + e.getMessage());
		}
	}	
	
	public void testSelectOfCustomersWholeTable() throws RollbackException {
		String sql = "SELECT * from Customers";
		try {
			BDResultSet results = runSql(sql);
			assertEquals(30, results.getData().size());
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("BDParseException: " + e.getMessage());
		} catch (SemanticException e) {
			e.printStackTrace();
			fail("SemanticException: " + e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail("InterruptedException: " + e.getMessage());
		}
	}		

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
}