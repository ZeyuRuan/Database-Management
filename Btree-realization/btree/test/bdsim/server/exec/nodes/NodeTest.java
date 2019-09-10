package bdsim.server.exec.nodes;

import junit.framework.TestCase;

public class NodeTest extends TestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}

	public void testSelectEqual() {
		NodeStatement select1 = new NodeSelect(false, false,
				new NodeColumnStar(), new NodeTableList("members", null, BDJoinType.NONE, null),
				null, null);
		NodeStatement select2 = new NodeSelect(false, false,
				new NodeColumnStar(), new NodeTableList("members", null, BDJoinType.NONE,null),
				null, null);
		assertTrue(select1.equals(select2));
	}

	public void testSelectNotEqual() {
		NodeStatement select1 = new NodeSelect(false, false,
				new NodeColumnStar(), new NodeTableList("membersx", null,
						BDJoinType.NONE, null), null, null);
		NodeStatement select2 = new NodeSelect(false, false,
				new NodeColumnStar(), new NodeTableList("members", null,
						BDJoinType.NONE, null), null, null);
		assertFalse(select1.equals(select2));
		NodeDelete delete1 = new NodeDelete(true, "members", null);
		NodeDelete delete2 = new NodeDelete(false, "members", null);
		assertFalse(delete1.equals(delete2));
	}

	public void testUpdateEqual() {
		NodeStatement update1 = new NodeUpdate("members",
				new NodeAssignmentList("name", new NodeValueLiteral("Bill"),
						null), new NodeConditionList(new NodeCondition(
						new NodeConditionValueColumn("last_name"), new NodeCondOpEqual(),
						new NodeConditionValueLiteral("Pijewski")), null, null));

		NodeStatement update2 = new NodeUpdate("members",
				new NodeAssignmentList("name", new NodeValueLiteral("Bill"),
						null), new NodeConditionList(new NodeCondition(
						new NodeConditionValueColumn("last_name"), new NodeCondOpEqual(),
						new NodeConditionValueLiteral("Pijewski")), null, null));
		assertTrue(update1.equals(update2));
	}

	public void testUpdateNotEqual() {
		NodeStatement update1 = new NodeUpdate("members",
				new NodeAssignmentList("name", new NodeValueLiteral("Bill"),
						new NodeAssignmentList("name", new NodeValueLiteral(
								"Bill"), null)), new NodeConditionList(
						new NodeCondition(new NodeConditionValueColumn("last_name"), new NodeCondOpEqual(),
								new NodeConditionValueLiteral("Pijewski")), null, null));

		NodeStatement update2 = new NodeUpdate("members",
				new NodeAssignmentList("name", new NodeValueLiteral("Bill"),
						null), new NodeConditionList(new NodeCondition(
						new NodeConditionValueColumn("last_name"), new NodeCondOpEqual(),
						new NodeConditionValueLiteral("Pijewski")), null, null));
		assertFalse(update1.equals(update2));
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
}
