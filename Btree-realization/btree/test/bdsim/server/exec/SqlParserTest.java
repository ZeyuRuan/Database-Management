package bdsim.server.exec;

import java.util.List;

import junit.framework.TestCase;

import bdsim.server.exec.nodes.*;

/**
 * Parser tests.
 * 
 * @author wpijewsk
 * @revision $Id: SqlParserTest.java 274 2007-01-20 02:56:25Z wpijewsk $
 */
public class SqlParserTest extends TestCase {

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

	public void testBasic() {
		try {
			String text = "SELECT * FROM members";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), null, null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertEquals(expected, actualList.get(0));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
	}

	public void testCommas() {
		try {
			String text = "SELECT col1,col2 as  name1    , col3 FROM table1 , table2 ,table3";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnList("col1", null, new NodeColumnList("col2",
							"name1", new NodeColumnList("col3", null, null))),
					new NodeTableList("table1", null, BDJoinType.NONE,
							new NodeTableList("table2", null, BDJoinType.NONE,
									new NodeTableList("table3", null,
											BDJoinType.NONE, null))), null, null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertEquals(expected, actualList.get(0));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
	}

	public void testConditionComplex() {
		try {
			String text = "SELECT * FROM members WHERE col1 LIKE '%Bill%' AND col2 > 10";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpLike(),
									new NodeConditionValueLiteral("%Bill%")),
							new NodeBoolOpAnd(),
							new NodeConditionList(new NodeCondition(
									new NodeConditionValueColumn("col2"),
									new NodeCondOpGreaterThan(),
									new NodeConditionValueNumber(10)), null,
									null)), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members WHERE col1 = 'Bill%' OR col2 >= 10.3";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpEqual(),
									new NodeConditionValueLiteral("Bill%")),
							new NodeBoolOpOr(),
							new NodeConditionList(new NodeCondition(
									new NodeConditionValueColumn("col2"),
									new NodeCondOpGreaterThanOrEqual(),
									new NodeConditionValueNumber(10.3)), null,
									null)), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members WHERE col1 = 'Bill%' OR col2 >= 10.3 AND col3 <= -1.43";
			NodeStatement expected = new NodeSelect(
					false,
					false,
					new NodeColumnStar(),
					new NodeTableList("members", null, BDJoinType.NONE, null),
					new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpEqual(),
									new NodeConditionValueLiteral("Bill%")),
							new NodeBoolOpOr(),
							new NodeConditionList(
									new NodeCondition(
											new NodeConditionValueColumn("col2"),
											new NodeCondOpGreaterThanOrEqual(),
											new NodeConditionValueNumber(10.3)),
									new NodeBoolOpAnd(),
									new NodeConditionList(
											new NodeCondition(
													new NodeConditionValueColumn(
															"col3"),
													new NodeCondOpLessThanOrEqual(),
													new NodeConditionValueNumber(
															-1.43)), null, null))),
					null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
	}

	public void testConditionSimple() {
		try {
			String text = "SELECT * FROM members WHERE col1 > 21.3";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(

					new NodeCondition(new NodeConditionValueColumn("col1"),
							new NodeCondOpGreaterThan(),
							new NodeConditionValueNumber(21.3))

					, null, null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members WHERE 21.3 < col1";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(
									new NodeConditionValueNumber(21.3),
									new NodeCondOpLessThan(),
									new NodeConditionValueColumn("col1")),
							null, null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members WHERE col1 < 21.3";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpLessThan(),
									new NodeConditionValueNumber(21.3)), null,
							null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members WHERE col1 >= 21.3";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"),
									new NodeCondOpGreaterThanOrEqual(),
									new NodeConditionValueNumber(21.3)), null,
							null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members WHERE col1 = 'Bill'";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpEqual(),
									new NodeConditionValueLiteral("Bill")),
							null, null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members, accounts WHERE members.id = accounts.id";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, new NodeTableList("accounts", null,
									BDJoinType.NONE, null)),
					new NodeConditionList(new NodeCondition(
							new NodeConditionValueColumn("members.id"),
							new NodeCondOpEqual(),
							new NodeConditionValueColumn("accounts.id")), null,
							null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members WHERE col1 LIKE '%Bill%'";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpLike(),
									new NodeConditionValueLiteral("%Bill%")),
							null, null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
	}

	/**
	 * Tests DELETE expressions.
	 */
	public void testDelete() {

		String stdListText = "WHERE col1 LIKE '%Bill%' AND col2 > 10";
		NodeConditionList stdList = new NodeConditionList(new NodeCondition(
				new NodeConditionValueColumn("col1"), new NodeCondOpLike(),
				new NodeConditionValueLiteral("%Bill%")), new NodeBoolOpAnd(),
				new NodeConditionList(new NodeCondition(
						new NodeConditionValueColumn("col2"),
						new NodeCondOpGreaterThan(),
						new NodeConditionValueNumber(10)), null, null));

		try {
			String text = "DELETE * from members  " + stdListText;
			NodeStatement expected = new NodeDelete(true, "members", stdList);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "DELETE from members  " + stdListText;
			NodeStatement expected = new NodeDelete(false, "members", stdList);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "DELETE from members";
			NodeStatement expected = new NodeDelete(false, "members", null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "DELETE members " + stdListText;
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError01() {
		try {
			String text = "select where from";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError02() {
		try {
			String text = "from";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError03() {
		try {
			String text = "where";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError04() {
		try {
			String text = "select ,col1, col2";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError05() {
		try {
			String text = "select * from , table1";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError06() {
		try {
			String text = ", select * from table1";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError07() {
		try {
			String text = "select all distinct from table1";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError08() {
		try {
			String text = "select distinct all from table1";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError09() {
		try {
			String text = "select distinct from table1 where col1 = 'Bill' extra";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError10() {
		try {
			String text = "select * as name from table1";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError11() {
		try {
			String text = "select col1 as <> from table1";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	public void testError12() {
		try {
			String text = "select col1 as name1, * from table1";
			m_parser.parse(text);
			fail("Did not throw BDParseException");
		} catch (BDParseException e) {
			assertTrue(true);
		}
	}

	/**
	 * Tests INSERT expressions.
	 */
	public void testInsert() {
		try {
			String text = "insert into members (age, name, sex) values (20.8, 'Bill', 'M')";
			NodeStatement expected = new NodeInsert("members",
					new NodeIdList("age", new NodeIdList("name",
							new NodeIdList("sex", null))), new NodeValueList(
							new NodeValueNumber(20.8), new NodeValueList(
									new NodeValueLiteral("Bill"),
									new NodeValueList(
											new NodeValueLiteral("M"), null))));
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
	}

	public void testOrderByComplex() {
		try {
			String text = "SELECT distinct col1 FROM table1 order by col2 desc, col1 asc, col3 desc";
			NodeStatement expected = new NodeSelect(false, true,
					new NodeColumnList("col1", null, null), new NodeTableList(
							"table1", null, BDJoinType.NONE, null), null,
					new NodeOrderByList("col2", false, new NodeOrderByList(
							"col1", true, new NodeOrderByList("col3", false,
									null))));
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT distinct col1 FROM table1 WHERE col1 LIKE '%Bill%' AND col2 > 10 ORDER BY col2 desc, col1 asc, col3 desc";
			NodeStatement expected = new NodeSelect(false, true,
					new NodeColumnList("col1", null, null), new NodeTableList(
							"table1", null, BDJoinType.NONE, null),
					new NodeConditionList(new NodeCondition(
							new NodeConditionValueColumn("col1"),
							new NodeCondOpLike(),
							new NodeConditionValueLiteral("%Bill%")),
							new NodeBoolOpAnd(),
							new NodeConditionList(new NodeCondition(
									new NodeConditionValueColumn("col2"),
									new NodeCondOpGreaterThan(),
									new NodeConditionValueNumber(10)), null,
									null)), new NodeOrderByList("col2", false,
							new NodeOrderByList("col1", true,
									new NodeOrderByList("col3", false, null))));
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
	}

	public void testOrderBySimple() {
		try {
			String text = "SELECT distinct col1 FROM table1 order by col2 asc";
			NodeStatement expected = new NodeSelect(false, true,
					new NodeColumnList("col1", null, null), new NodeTableList(
							"table1", null, BDJoinType.NONE, null), null,
					new NodeOrderByList("col2", true, null));
			List<NodeStatement> actualList = m_parser.parse(text);
			assertEquals(expected, actualList.get(0));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT distinct col1 FROM table1 order by col2 desc";
			NodeStatement expected = new NodeSelect(false, true,
					new NodeColumnList("col1", null, null), new NodeTableList(
							"table1", null, BDJoinType.NONE, null), null,
					new NodeOrderByList("col2", false, null));
			List<NodeStatement> actualList = m_parser.parse(text);
			assertEquals(expected, actualList.get(0));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "SELECT * FROM members WHERE col1 > 21.3 ORDER BY col1 desc";
			NodeStatement expected = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpGreaterThan(),
									new NodeConditionValueNumber(21.3)), null,
							null), new NodeOrderByList("col1", false, null));
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
	}

	/**
	 * Tests UPDATE expressions.
	 */
	public void testUpdate() {
		try {
			String text = "update members set name='Bill' where last_name='Pijewski'";
			NodeStatement expected = new NodeUpdate("members",
					new NodeAssignmentList("name",
							new NodeValueLiteral("Bill"), null),
					new NodeConditionList(new NodeCondition(
							new NodeConditionValueColumn("last_name"),
							new NodeCondOpEqual(),
							new NodeConditionValueLiteral("Pijewski")), null,
							null));
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "update members set name='Bill', age=21 where last_name='Pijewski'";
			NodeStatement expected = new NodeUpdate("members",
					new NodeAssignmentList("name",
							new NodeValueLiteral("Bill"),
							new NodeAssignmentList("age", new NodeValueNumber(
									21), null)), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"last_name"), new NodeCondOpEqual(),
									new NodeConditionValueLiteral("Pijewski")),
							null, null));
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "update members set name='Bill', age=21 ";
			NodeStatement expected = new NodeUpdate("members",
					new NodeAssignmentList("name",
							new NodeValueLiteral("Bill"),
							new NodeAssignmentList("age", new NodeValueNumber(
									21), null)), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
	}
	
	public void testMultiquery() {
		try {
			String text = "begin transaction update members set name='Bill', age=21 commit transaction";
			NodeStatement expected = new NodeUpdate("members",
					new NodeAssignmentList("name",
							new NodeValueLiteral("Bill"),
							new NodeAssignmentList("age", new NodeValueNumber(
									21), null)), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected.equals(actualList.get(0)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}
		
		try {
			String text = "begin transaction update members set name='Bill', age=21; SELECT * FROM members WHERE col1 > 21.3 commit transaction";
			NodeStatement expected0 = new NodeUpdate("members",
					new NodeAssignmentList("name",
							new NodeValueLiteral("Bill"),
							new NodeAssignmentList("age", new NodeValueNumber(
									21), null)), null);
			NodeStatement expected1 = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpGreaterThan(),
									new NodeConditionValueNumber(21.3))
							, null, null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected0.equals(actualList.get(0)));
			assertTrue(expected1.equals(actualList.get(1)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}

		try {
			String text = "update members set name='Bill', age=21; SELECT * FROM members WHERE col1 > 21.3 ; SELECT * FROM members WHERE col1 > 21.3 ; SELECT * FROM members WHERE col1 > 21.3 ";
			NodeStatement expected0 = new NodeUpdate("members",
					new NodeAssignmentList("name",
							new NodeValueLiteral("Bill"),
							new NodeAssignmentList("age", new NodeValueNumber(
									21), null)), null);
			NodeStatement expected1 = new NodeSelect(false, false,
					new NodeColumnStar(), new NodeTableList("members", null,
							BDJoinType.NONE, null), new NodeConditionList(
							new NodeCondition(new NodeConditionValueColumn(
									"col1"), new NodeCondOpGreaterThan(),
									new NodeConditionValueNumber(21.3))
							, null, null), null);
			List<NodeStatement> actualList = m_parser.parse(text);
			assertTrue(expected0.equals(actualList.get(0)));
			assertTrue(expected1.equals(actualList.get(1)));
			assertTrue(expected1.equals(actualList.get(2)));
			assertTrue(expected1.equals(actualList.get(3)));
		} catch (BDParseException e) {
			e.printStackTrace();
			fail("Caught unexpected BDParseException");
		}		
	}
}
