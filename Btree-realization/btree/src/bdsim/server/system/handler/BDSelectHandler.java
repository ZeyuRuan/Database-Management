package bdsim.server.system.handler;

import java.util.List;

import org.apache.log4j.Logger;

import bdsim.server.exec.BDCondition;
import bdsim.server.exec.BDConditionList;
import bdsim.server.exec.BDPlan;
import bdsim.server.exec.BDQueryType;
import bdsim.server.exec.BDTableColumnPair;
import bdsim.server.exec.nodes.BDCondOpType;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDSystemResultSet;
import bdsim.server.system.BDTable;
import bdsim.server.system.concurrency.RollbackException;

/**
 * @author dclee
 */
public class BDSelectHandler implements BDHandler {

	static Logger logger = Logger.getLogger(BDSelectHandler.class);
	private BDPlan m_plan;
	private BDConditionList m_conditions;
	private List<String> m_tables;

	public BDSelectHandler(BDPlan p) {
		m_plan = p;
		m_conditions = p.getConditions();
		m_tables = p.getTables();
	}

	public BDSystemResultSet execute() throws InterruptedException, RollbackException {

		BDSystemResultSet selected;
		BDHandler handler, handler2;

		BDTable t1, t2;
		BDSystemResultSet rs1, rs2;

		if (m_tables.isEmpty()) {
			Logger.getLogger(BDSelectHandler.class).error(
					"Tables are empty. Cannot execute SELECT handler.");
			System.err.println("Fatal error. See log.");
			System.exit(-1);
		} else {
			logger.debug("Number of tables: " + m_tables.size());
		}
		if (m_tables.size() == 1) {
			if (m_conditions == null || m_conditions.getNumConditions() == 0) {
				selected = selectFromSingleTable(null);
			} else {
				selected = selectFromSingleTable(m_conditions.getCondition());

				// TODO wpijewsk Why would this assume that selected is not null?
				if(selected == null) {
					return selected;
				}
				
				if (m_conditions.getNumConditions() == 1) {
					if (!(m_plan.isSelectAll() || m_plan.getQTtype() == BDQueryType.DELETE)) {
						assert(selected != null);
						assert(selected.getSchema() != null);
						handler = new BDProjectHandler(selected,
								BDHandlerFunctions.stripTables(m_plan
										.getColumnPairs()));
						selected = handler.execute();
					}
					if (!(m_plan.getOrderByList().isEmpty())) {
						handler = new BDOrderByHandler(selected, m_plan
								.getOrderByList());
						selected = handler.execute();
					}
					return selected;
				}

				boolean lastAnd = m_conditions.isAndedTogether();

				m_conditions = m_conditions.getRest();
				while (m_conditions.getNumConditions() > 1) {
					if (lastAnd) {
						logger.debug("Performing an intersect");
						handler = new BDIntersectHandler(selected,
								selectFromSingleTable(m_conditions
										.getCondition()));
						selected = handler.execute();
					} else {
						logger.debug("Performing a union");
						handler = new BDUnionHandler(selected,
								selectFromSingleTable(m_conditions
										.getCondition()));
						selected = handler.execute();
					}
					lastAnd = m_conditions.isAndedTogether();
					m_conditions = m_conditions.getRest();
				}
				if (lastAnd) {
					logger.debug("Performing an intersect");
					handler = new BDIntersectHandler(selected,
							selectFromSingleTable(m_conditions.getCondition()));
				} else {
					logger.debug("Performing a union");
					handler = new BDUnionHandler(selected,
							selectFromSingleTable(m_conditions.getCondition()));
				}
				selected = handler.execute();
			}
			if (!(m_plan.isSelectAll() || m_plan.getQTtype() == BDQueryType.DELETE)) {
				handler = new BDProjectHandler(selected, BDHandlerFunctions
						.stripTables(m_plan.getColumnPairs()));
				selected = handler.execute();
			}
			if (!(m_plan.getOrderByList().isEmpty())) {
				handler = new BDOrderByHandler(selected, m_plan
						.getOrderByList());
				selected = handler.execute();
			}
			return selected;
		}

		if (m_tables.size() >= 2) {
			if (m_conditions == null || m_conditions.getNumConditions() == 0) { // Cross
				// Product
				logger.debug("\n\nComputing cross product\n\n");

				t1 = BDSystem.tableManager.getTableByName(m_tables.get(0));
				rs1 = t1.getAllTuples();
				for (int j = 1; j < m_tables.size(); j++) {
					t2 = BDSystem.tableManager.getTableByName(m_tables.get(j));
					rs2 = t2.getAllTuples();
					handler = new BDCrossProductHandler(rs1, rs2);
					rs1 = handler.execute();
				}

				if (!(m_plan.isSelectAll())) {
					handler2 = new BDProjectHandler(rs1, BDHandlerFunctions
							.stripTables(m_plan.getColumnPairs()));
					return handler2.execute();
				} else
					return rs1;
			}
			// Else it's some other kind of join

			else {
				selected = selectFromSingleTable(m_conditions.getCondition());

				if (m_conditions.getNumConditions() == 1) {
					// if(m_plan.isSelectAll()) return selected;
					// handler = new BDProjectHandler(selected,
					// BDHandlerFunctions.stripTables(m_plan.getColumnPairs()));
					// selected = handler.execute();

					if (!(m_plan.isSelectAll() || m_plan.getQTtype() == BDQueryType.DELETE)) {
						handler = new BDProjectHandler(selected,
								BDHandlerFunctions.stripTables(m_plan
										.getColumnPairs()));
						selected = handler.execute();
					}
					if (!(m_plan.getOrderByList().isEmpty())) {
						handler = new BDOrderByHandler(selected, m_plan
								.getOrderByList());
						selected = handler.execute();
					}
					return selected;
				}

				boolean lastAnd = m_conditions.isAndedTogether();

				m_conditions = m_conditions.getRest();
				while (m_conditions.getNumConditions() > 1) {
					if (lastAnd) {
						// logger.debug("Performing an intersect");
						// handler = new BDIntersectHandler(selected,
						// selectFromSingleTable(m_conditions.getCondition()));
						// selected = handler.execute();
						selected = selectFromResultSet(m_conditions
								.getCondition(), selected);
					} else {
						logger.debug("Performing a union");
						handler = new BDUnionHandler(selected,
								selectFromSingleTable(m_conditions
										.getCondition()));
						selected = handler.execute();
					}

					lastAnd = m_conditions.isAndedTogether();
					m_conditions = m_conditions.getRest();
				}
				if (m_conditions.isAndedTogether()) {
					// logger.debug("Performing an intersect");
					selected = selectFromResultSet(m_conditions.getCondition(),
							selected);
				} else {
					logger.debug("Performing a union");
					handler = new BDUnionHandler(selected,
							selectFromSingleTable(m_conditions.getCondition()));
					selected = handler.execute();
				}
			}

			if (!(m_plan.isSelectAll() || m_plan.getQTtype() == BDQueryType.DELETE)) {
				handler = new BDProjectHandler(selected, BDHandlerFunctions
						.stripTables(m_plan.getColumnPairs()));
				selected = handler.execute();
			}
			logger.debug("Checking order-by: ");
			if (!(m_plan.getOrderByList().isEmpty())) {
				logger.debug("Order-by found");
				handler = new BDOrderByHandler(selected, m_plan
						.getOrderByList());
				selected = handler.execute();
			}
			return selected;
		}
		return null;
	}

	private BDSystemResultSet selectFromResultSet(BDCondition c,
			BDSystemResultSet original) throws InterruptedException, RollbackException {
		// BDCondition c;
		BDTableColumnPair conditionTablePair;
		Object conditionOtherArg;
		BDCondOpType cType;

		if (m_conditions == null) {
			return BDSystem.tableManager.getTableByName(
					m_plan.getTables().get(0)).getAllTuples();
		}

		// c = m_conditions.getCondition();

		if (c.getLhsType() != BDCondition.ConditionValueType.COLUMN
				&& c.getRhsType() != BDCondition.ConditionValueType.COLUMN) {
			// TODO throw a meaningful exception about invalid comparison
		}
		// One column on left side
		else if (c.getLhsType() == BDCondition.ConditionValueType.COLUMN) {
			conditionTablePair = (BDTableColumnPair) c.getLhs();
			conditionOtherArg = c.getRhs();
			cType = c.getCondOp();
			if (cType.equals(BDCondOpType.EQ))
				return original.getTuplesByValue(
						conditionTablePair.getColumn(),
						(Comparable) conditionOtherArg);
			else
				return original.getTuplesByRange(BDHandlerFunctions
						.condToRange(cType), conditionTablePair.getColumn(),
						(Comparable) conditionOtherArg);

		}
		// One column on right side
		else {
			conditionTablePair = (BDTableColumnPair) c.getRhs();
			conditionOtherArg = c.getLhs();
			cType = c.getCondOp();
			if (cType.equals(BDCondOpType.EQ))
				return original.getTuplesByValue(
						conditionTablePair.getColumn(),
						(Comparable) conditionOtherArg);
			else
				return original.getTuplesByRange(BDHandlerFunctions
						.invertRange(BDHandlerFunctions.condToRange(cType)),
						conditionTablePair.getColumn(),
						(Comparable) conditionOtherArg);
		}
		return null;
	}

	private BDSystemResultSet selectFromSingleTable(BDCondition cond)
			throws InterruptedException, RollbackException {

		BDTableColumnPair conditionTablePair;
		BDTableColumnPair conditionTablePair2;
		BDTable table, table2;
		Object conditionOtherArg;
		BDCondOpType condType;
		BDHandler handler;

		if (m_conditions == null) {
			return BDSystem.tableManager.getTableByName(
					m_plan.getTables().get(0)).getAllTuples();
		}

		if (cond.getLhsType() != BDCondition.ConditionValueType.COLUMN
				&& cond.getRhsType() != BDCondition.ConditionValueType.COLUMN) {
			// TODO throw a meaningful exception about invalid comparison
			logger
					.error("Trying to compare two objects with different schemas");
			System.exit(-1);
		}

		// We need to join tables if there are two columns compared
		if (cond.getLhsType() == BDCondition.ConditionValueType.COLUMN
				&& cond.getRhsType() == BDCondition.ConditionValueType.COLUMN) {
			conditionTablePair = (BDTableColumnPair) cond.getLhs();
			table = BDSystem.tableManager.getTableByName(conditionTablePair
					.getTable());

			logger.debug("Column 1:" + conditionTablePair.getColumn());

			conditionTablePair2 = (BDTableColumnPair) cond.getRhs();
			table2 = BDSystem.tableManager.getTableByName(conditionTablePair2
					.getTable());

			logger.debug("Column 2:" + conditionTablePair2.getColumn());

			condType = cond.getCondOp();
			if (condType.equals(BDCondOpType.EQ)) {
				handler = new BDNaturalJoinHandler(table.getAllTuples(), table2
						.getAllTuples(), conditionTablePair.getColumn(),
						conditionTablePair2.getColumn());
				return handler.execute();
			} else {
				assert false;
			}

		}
		
		// One column on left side
		else if (cond.getLhsType() == BDCondition.ConditionValueType.COLUMN) {
			conditionTablePair = (BDTableColumnPair) cond.getLhs();

			logger.debug("Column on left: " + conditionTablePair);
			logger.debug("op: " + cond.getCondOp());
			logger.debug("other arg: " + cond.getRhs());

			table = BDSystem.tableManager.getTableByName(conditionTablePair
					.getTable());
			conditionOtherArg = cond.getRhs();
			condType = cond.getCondOp();
			if (condType.equals(BDCondOpType.EQ)) {
				logger.debug("Calling B+ Tree. T: " + table);
				return table.getTuplesByValue(conditionTablePair.getColumn(),
						conditionOtherArg);
			} else {
				if (!table.isPrimaryKey(conditionTablePair.getColumn())) {
					logger.debug("\nGetting from result set:");
					return table.getAllTuples().getTuplesByRange(
							BDHandlerFunctions.condToRange(condType),
							conditionTablePair.getColumn(),
							(Comparable) conditionOtherArg);
				} else {
					BDSystemResultSet results = table.getTuplesByRange(
							BDHandlerFunctions.condToRange(condType),
							conditionTablePair.getColumn(),
							(Comparable) conditionOtherArg);
					if(results == null || results.getData().size() == 0) {
						return null;
					} else {
						return results;
					}
				}
			}
		}
		// One column on right side
		else {
			conditionTablePair = (BDTableColumnPair) cond.getRhs();

			logger.debug("Column on right: " + conditionTablePair);
			logger.debug("op: " + cond.getCondOp());
			logger.debug("other arg: " + cond.getLhs());

			table = BDSystem.tableManager.getTableByName(conditionTablePair
					.getTable());
			conditionOtherArg = cond.getLhs();
			condType = cond.getCondOp();
			if (condType.equals(BDCondOpType.EQ))
				return table.getTuplesByValue(conditionTablePair.getColumn(),
						conditionOtherArg);
			else {
				if (!table.isPrimaryKey(conditionTablePair.getColumn())) {
					return table.getAllTuples().getTuplesByRange(
							BDHandlerFunctions.invertRange(BDHandlerFunctions
									.condToRange(condType)),
							conditionTablePair.getColumn(),
							(Comparable) conditionOtherArg);
				} else
					return table.getTuplesByRange(
							BDHandlerFunctions.invertRange(BDHandlerFunctions
									.condToRange(condType)), conditionTablePair
									.getColumn(),
							(Comparable) conditionOtherArg);
			}
		}
		return null;
	}
}
