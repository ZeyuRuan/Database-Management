package bdsim.server.exec;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import bdsim.server.exec.BDCondition.ConditionValueType;
import bdsim.server.exec.BDSemanticVisitor.BDNameMappings;
import bdsim.server.exec.BDSemanticVisitor.SemanticException;
import bdsim.server.exec.nodes.BDCondOpType;
import bdsim.server.exec.nodes.NodeStatement;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDTableManager.XmlException;

/**
 * Tests the plan generation for select, insert, and delete statements.
 * 
 * @author wpijewsk
 * @revision $Id: BDPlanTest.java 301 2007-01-22 18:32:38Z acath $
 */
public final class BDPlanTest extends TestCase {

	private BDSqlParser m_parser;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		m_parser = new BDSqlParser();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testDeleteSuccessCases() {

		try {
			BDSystem.tableManager.readFromXmlFile("test/files/Bank.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/Bank/xml");
		}

		try {
			String text = "DELETE * from Customers";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = true;
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Customers");
					expected.m_qtype = BDQueryType.DELETE;

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			fail("Caught unexpected BDParseException:");
			e.printStackTrace();
		} catch (SemanticException e) {
			fail("Caught unexpected SemanticException");
			e.printStackTrace();
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}


	}

	public void testErrorCases() {

		try {
			BDSystem.tableManager
					.readFromXmlFile("test/files/SmallTwoTable.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/SmallTwoTable.xml");
		}

		try {
			String text = "SELECT b.Num1 as n1, Num2 as n2 from Test1 as a, Test2 as b";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (smtcVisitor.resolveNames()) {
					fail("Erroneously resolved b.Num1 (Num1 is not a field of b).");
				}
			}
		} catch (BDParseException e) {
			fail("Caught unexpected BDParseException:");
			e.printStackTrace();
		} catch (SemanticException e) {
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}

		try {
			String text = "SELECT Test2.Num1 as n1, Num2 as n2 from Test1 as a, Test2 as b";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (smtcVisitor.resolveNames()) {
					fail("Erroneously resolved b.Num1 (Num1 is not a field of b).");
				}
			}
		} catch (BDParseException e) {
			fail("Caught unexpected BDParseException:");
			e.printStackTrace();
		} catch (SemanticException e) {
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}
	}
	
	public void testInsert1() {
		try {
			BDSystem.tableManager.readFromXmlFile("test/files/Bank.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/Bank/xml");
		}

		try {
			String text = "insert into Customers values (41., 'AA', 'BB', '087-34-2232')";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = false;
					expected.m_qtype = BDQueryType.INSERT;
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Customers");
					expected.m_data = new HashMap<BDTableColumnPair, Object>();

					expected.m_data.put(new BDTableColumnPair("Customers",
							"last_name"), new String("BB"));
					expected.m_data.put(new BDTableColumnPair("Customers",
							"first_name"), new String("AA"));
					expected.m_data.put(
							new BDTableColumnPair("Customers", "id"),
							new Double(41.));
					expected.m_data.put(new BDTableColumnPair("Customers",
							"SSN"), new String("087-34-2232"));
					expected.m_columnPairs = new LinkedList<BDTableColumnPair>();
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "id"));
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "first_name"));
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "last_name"));
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "SSN"));

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException:");
		} catch (SemanticException e) {
			e.printStackTrace();
			fail("Caught unexpected SemanticException");
		} catch (Exception e) {
			e.printStackTrace();
			fail("Caught unexpected Exception");
		}	
	}
	
	public void testInsert2() { 
		try {
			BDSystem.tableManager.readFromXmlFile("test/files/Bank.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/Bank/xml");
		}
		
		try {
			String text = "insert into Customers (last_name, SSN)  values ('BB', '087-34-2232')";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = false;
					expected.m_qtype = BDQueryType.INSERT;
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Customers");
					expected.m_data = new HashMap<BDTableColumnPair, Object>();

					expected.m_data.put(new BDTableColumnPair("Customers",
							"last_name"), new String("BB"));
					expected.m_data.put(new BDTableColumnPair("Customers",
							"SSN"), new String("087-34-2232"));
					
					expected.m_columnPairs = new LinkedList<BDTableColumnPair>();
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "last_name"));
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "SSN"));

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException: " + e.getMessage());
		} catch (SemanticException e) {
			e.printStackTrace();
			fail("Caught unexpected SemanticException: " + e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Caught unexpected Exception: " + e.getMessage());
		}			
	}
	
	public void testInsert3() { 
		try {
			BDSystem.tableManager.readFromXmlFile("test/files/Bank.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/Bank/xml");
		}		
		
		try {
			String text = "insert into Customers (id, first_name, last_name, SSN)  values (42., 'AA', 'BB', '087-34-2232')";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = false;
					expected.m_qtype = BDQueryType.INSERT;
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Customers");
					expected.m_data = new HashMap<BDTableColumnPair, Object>();

					expected.m_data.put(new BDTableColumnPair("Customers",
							"last_name"), new String("BB"));
					expected.m_data.put(new BDTableColumnPair("Customers",
							"first_name"), new String("AA"));
					expected.m_data.put(
							new BDTableColumnPair("Customers", "id"),
							new Double(42.));
					expected.m_data.put(new BDTableColumnPair("Customers",
							"SSN"), new String("087-34-2232"));
					expected.m_columnPairs = new LinkedList<BDTableColumnPair>();
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "id"));
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "first_name"));
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "last_name"));
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "SSN"));

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException: " + e.getMessage());
		} catch (SemanticException e) {
			e.printStackTrace();
			fail("Caught unexpected SemanticException: " + e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Caught unexpected Exception: " + e.getMessage());
		}			
	}

	public void testInsert4() { 
		try {
			BDSystem.tableManager.readFromXmlFile("test/files/Bank.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/Bank/xml");
		}		
		
		try {
			String text = "insert into Customers (id)  values (43.)";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = false;
					expected.m_qtype = BDQueryType.INSERT;
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Customers");
					expected.m_data = new HashMap<BDTableColumnPair, Object>();

					expected.m_data.put(
							new BDTableColumnPair("Customers", "id"),
							new Double(43.));

					expected.m_columnPairs = new LinkedList<BDTableColumnPair>();
					expected.m_columnPairs.addLast(new BDTableColumnPair(
							"Customers", "id"));

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException: " + e.getMessage());
		} catch (SemanticException e) {
			e.printStackTrace();
			fail("Caught unexpected SemanticException: " + e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Caught unexpected Exception: " + e.getMessage());
		}		
	}

	public void testMappings() {

		try {
			BDSystem.tableManager
					.readFromXmlFile("test/files/SmallTwoTable.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/SmallTwoTable.xml");
		}

		try {
			String text = "SELECT Num1 as n1, Num2 as n2 from Test1 as a, Test2 as b";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();

					Map<String, BDTableColumnPair> colAbbrs = new HashMap<String, BDTableColumnPair>();
					colAbbrs.put("n1", new BDTableColumnPair("Test1", "Num1"));
					colAbbrs.put("n2", new BDTableColumnPair("Test1", "Num2"));
					Map<String, String> tblAbbrs = new HashMap<String, String>();
					tblAbbrs.put("a", "Test1");
					tblAbbrs.put("b", "Test2");
					assertEquals(colAbbrs, mappings.getColAbbrs());
					assertEquals(tblAbbrs, mappings.getTblAbbrs());
				}
			}
		} catch (BDParseException e) {
			fail("Caught unexpected BDParseException:");
			e.printStackTrace();
		} catch (SemanticException e) {
			fail("Caught unexpected SemanticException");
			e.printStackTrace();
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}

		try {
			String text = "SELECT b.Num3 as s1, b.Num4 as s2 from Test1 as a, Test2 as b";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();

					Map<String, BDTableColumnPair> colAbbrs = new HashMap<String, BDTableColumnPair>();
					colAbbrs.put("s1", new BDTableColumnPair("Test2", "Num3"));
					colAbbrs.put("s2", new BDTableColumnPair("Test2", "Num4"));
					Map<String, String> tblAbbrs = new HashMap<String, String>();
					tblAbbrs.put("a", "Test1");
					tblAbbrs.put("b", "Test2");
					assertEquals(colAbbrs, mappings.getColAbbrs());
					assertEquals(tblAbbrs, mappings.getTblAbbrs());
				}
			}
		} catch (BDParseException e) {
			fail("Caught unexpected BDParseException:");
			e.printStackTrace();
		} catch (SemanticException e) {
			fail("Caught unexpected SemanticException");
			e.printStackTrace();
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}

	}

	public void testSelectSuccessCases() {

		try {
			BDSystem.tableManager
					.readFromXmlFile("test/files/SmallTwoTable.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/SmallTwoTable.xml");
		}

		try {
			String text = "SELECT * from Test1, Test2 where Test1.Num1 = Test1.Num2";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = true;
					BDCondition condition = new BDCondition(null,
							BDCondOpType.EQ, null);
					condition.setLhs(new BDTableColumnPair("Test1", "Num1"),
							ConditionValueType.COLUMN);
					condition.setRhs(new BDTableColumnPair("Test1", "Num2"),
							ConditionValueType.COLUMN);
					expected.m_conditions = new BDConditionList(condition,
							true, null);
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Test1");
					expected.m_tables.add("Test2");
					expected.m_qtype = BDQueryType.SELECT;

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			fail("Caught unexpected BDParseException:");
			e.printStackTrace();
		} catch (SemanticException e) {
			fail("Caught unexpected SemanticException");
			e.printStackTrace();
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}

		try {
			String text = "SELECT * from Test1 as A, Test2 where A.Num1 = A.Num2";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = true;
					BDCondition condition = new BDCondition(null,
							BDCondOpType.EQ, null);
					condition.setLhs(new BDTableColumnPair("Test1", "Num1"),
							ConditionValueType.COLUMN);
					condition.setRhs(new BDTableColumnPair("Test1", "Num2"),
							ConditionValueType.COLUMN);
					expected.m_conditions = new BDConditionList(condition,
							true, null);
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Test1");
					expected.m_tables.add("Test2");
					expected.m_qtype = BDQueryType.SELECT;

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			fail("Caught unexpected BDParseException:");
			e.printStackTrace();
		} catch (SemanticException e) {
			fail("Caught unexpected SemanticException");
			e.printStackTrace();
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}

		try {
			String text = "SELECT A.Num1 from Test1 as A, Test2 where A.Num1 = A.Num2";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = false;
					BDCondition condition = new BDCondition(null,
							BDCondOpType.EQ, null);
					condition.setLhs(new BDTableColumnPair("Test1", "Num1"),
							ConditionValueType.COLUMN);
					condition.setRhs(new BDTableColumnPair("Test1", "Num2"),
							ConditionValueType.COLUMN);
					expected.m_conditions = new BDConditionList(condition,
							true, null);
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Test1");
					expected.m_tables.add("Test2");
					expected.m_qtype = BDQueryType.SELECT;
					expected.m_columnPairs.add(new BDTableColumnPair("Test1",
							"Num1"));

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			fail("Caught unexpected BDParseException:");
			e.printStackTrace();
		} catch (SemanticException e) {
			fail("Caught unexpected SemanticException");
			e.printStackTrace();
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}

		try {
			BDSystem.tableManager.readFromXmlFile("test/files/Bank.xml");
		} catch (XmlException e) {
			e.printStackTrace();
			fail("Error when reading test/files/SmallTwoTable.xml");
		}

		try {
			String text = "select *  from Accounts WHERE id < 8.0 AND amount < 512.00 OR cust_id > 5.0";
			List<NodeStatement> stmtList = m_parser.parse(text);
			for (NodeStatement stmt : stmtList) {
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				stmt.visit(smtcVisitor);
				if (!smtcVisitor.resolveNames()) {
					fail("Could not resolve names.");
				} else {
					BDNameMappings mappings = smtcVisitor.getMappings();
					BDPlanner planner = new BDPlanner(stmt);
					BDPlan actual = planner.makePlan(mappings);

					BDCondition condition1 = new BDCondition(null,
							BDCondOpType.LT, null);
					condition1.setLhs(new BDTableColumnPair("Accounts", "id"),
							ConditionValueType.COLUMN);
					condition1.setRhs(new Double(8.0),
							ConditionValueType.NUMBER);

					BDCondition condition2 = new BDCondition(null,
							BDCondOpType.LT, null);
					condition2.setLhs(new BDTableColumnPair("Accounts",
							"amount"), ConditionValueType.COLUMN);
					condition2.setRhs(new Double(512.00),
							ConditionValueType.NUMBER);

					BDCondition condition3 = new BDCondition(null,
							BDCondOpType.GT, null);
					condition3.setLhs(new BDTableColumnPair("Accounts",
							"cust_id"), ConditionValueType.COLUMN);
					condition3.setRhs(new Double(5.0),
							ConditionValueType.NUMBER);

					BDPlan expected = new BDPlan(stmt, mappings);
					expected.m_selectAll = true;
					expected.m_conditions = new BDConditionList(
							condition1,
							true,
							new BDConditionList(condition2, false,
									new BDConditionList(condition3, true, null)));
					expected.m_tables = new LinkedList<String>();
					expected.m_tables.add("Accounts");
					expected.m_qtype = BDQueryType.SELECT;

					assertEquals(expected, actual);
				}
			}
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException:");
		} catch (SemanticException e) {
			fail("Caught unexpected SemanticException");
			e.printStackTrace();
		} catch (Exception e) {
			fail("Caught unexpected Exception");
			e.printStackTrace();
		}
	}
}
