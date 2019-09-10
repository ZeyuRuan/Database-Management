package bdsim.server.exec;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import bdsim.server.exec.nodes.BDJoinType;
import bdsim.server.exec.nodes.NodeAssignmentList;
import bdsim.server.exec.nodes.NodeBoolOp;
import bdsim.server.exec.nodes.NodeBoolOpAnd;
import bdsim.server.exec.nodes.NodeBoolOpOr;
import bdsim.server.exec.nodes.NodeColumnClause;
import bdsim.server.exec.nodes.NodeColumnList;
import bdsim.server.exec.nodes.NodeColumnStar;
import bdsim.server.exec.nodes.NodeCondOp;
import bdsim.server.exec.nodes.NodeCondOpEqual;
import bdsim.server.exec.nodes.NodeCondOpGreaterThan;
import bdsim.server.exec.nodes.NodeCondOpGreaterThanOrEqual;
import bdsim.server.exec.nodes.NodeCondOpLessThan;
import bdsim.server.exec.nodes.NodeCondOpLessThanOrEqual;
import bdsim.server.exec.nodes.NodeCondOpLike;
import bdsim.server.exec.nodes.NodeCondOpNotEqual;
import bdsim.server.exec.nodes.NodeCondition;
import bdsim.server.exec.nodes.NodeConditionList;
import bdsim.server.exec.nodes.NodeConditionValue;
import bdsim.server.exec.nodes.NodeConditionValueColumn;
import bdsim.server.exec.nodes.NodeConditionValueLiteral;
import bdsim.server.exec.nodes.NodeConditionValueNumber;
import bdsim.server.exec.nodes.NodeDelete;
import bdsim.server.exec.nodes.NodeIdList;
import bdsim.server.exec.nodes.NodeInsert;
import bdsim.server.exec.nodes.NodeOrderByList;
import bdsim.server.exec.nodes.NodeSelect;
import bdsim.server.exec.nodes.NodeStatement;
import bdsim.server.exec.nodes.NodeTableList;
import bdsim.server.exec.nodes.NodeUpdate;
import bdsim.server.exec.nodes.NodeValue;
import bdsim.server.exec.nodes.NodeValueList;
import bdsim.server.exec.nodes.NodeValueLiteral;
import bdsim.server.exec.nodes.NodeValueNumber;

/**
 * Parses a string of text into a SQL statement, using the minimal grammar
 * provided.
 * 
 * @author wpijewsk
 * @revision $Id: BDSqlParser.java 186 2006-05-14 03:36:31 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public class BDSqlParser {

	static Logger logger = Logger.getLogger(BDSqlParser.class);
	private BDSqlTokenizer m_tokenizer;

	/**
	 * Parses some text as a SQL expression, checking for parse errors. Checks
	 * items thought to be elements of the schema (column and table names)
	 * against the actual schema of the database. Supports multiquery operations
	 * using the 'BEGIN TRANSACTION' and 'COMMIT TRANSACTION' statements,
	 * 
	 * @param text The text to parse
	 * @return An list of abstract representations of the SQL statements.
	 * @throws BDParseException
	 */
	public List<NodeStatement> parse(String text) throws BDParseException {
		logger.debug("Parsing expression:\n\t" + text);

		LinkedList<NodeStatement> statements = new LinkedList<NodeStatement>();
		
		boolean moreTokens = true;
		m_tokenizer = new BDSqlTokenizer(text);
		m_tokenizer.advance();
		
		// Check for 'BEGIN TRANSACTION'
		if(m_tokenizer.token().getType() == BDTokenType.BEGIN ) {
			m_tokenizer.advance();
			if(m_tokenizer.token().getType() != BDTokenType.TRANSACTION) {
				throw new BDParseException("BEGIN is not followed by TRANSACTION");
			}
			m_tokenizer.advance();

			text = text.substring(text.toLowerCase().indexOf("transaction")
					+ "transaction".length());
		}

		while (moreTokens) {
			String curText = null;
			if(text.contains(";")) {
				curText = text.substring(0, text.indexOf(';'));
			} else {
				curText = text;
			}
			
			m_tokenizer = new BDSqlTokenizer(curText);
			m_tokenizer.advance();
			NodeStatement statement = null;

			if (m_tokenizer.token().getType() == BDTokenType.SELECT) {
				m_tokenizer.advance();
				statement = parseSelect();
			} else if (m_tokenizer.token().getType() == BDTokenType.UPDATE) {
				m_tokenizer.advance();
				statement = parseUpdate();
			} else if (m_tokenizer.token().getType() == BDTokenType.INSERT) {
				m_tokenizer.advance();
				statement = parseInsert();
			} else if (m_tokenizer.token().getType() == BDTokenType.DELETE) {
				m_tokenizer.advance();
				statement = parseDelete();
			} else {
				throw new BDParseException(
						"Expression does not start with legal keyword");
			}

			statements.addLast(statement);
			
			// Modify text to eat up parsed part
			if (text.contains(";")) {
				text = text.substring(text.toLowerCase().indexOf(";")
						+ ";".length());
			} else if (text.toLowerCase().contains("commit")) {
				text = text.substring(text.toLowerCase().indexOf("commit"));
				moreTokens = false;
			} else if (text.toLowerCase().contains("rollback")) {
				text = text.substring(text.toLowerCase().indexOf("rollback"));
				moreTokens = false;
			} else {
				text = "";
				moreTokens = false;
			}
		}
		
		m_tokenizer = new BDSqlTokenizer(text);
		m_tokenizer.advance();
		
		// Check for 'COMMIT/ROLLBACK TRANSACTION'
		if(m_tokenizer.token().getType() == BDTokenType.COMMIT ) {
			m_tokenizer.advance();
			if(m_tokenizer.token().getType() != BDTokenType.TRANSACTION) {
				throw new BDParseException("COMMIT is not followed by TRANSACTION");
			}
			m_tokenizer.advance();
		} else if ( m_tokenizer.token().getType() == BDTokenType.ROLLBACK){
			m_tokenizer.advance();
			if(m_tokenizer.token().getType() != BDTokenType.TRANSACTION) {
				throw new BDParseException("ROLLBACK is not followed by TRANSACTION");
			}
			m_tokenizer.advance();			
		}

		if (m_tokenizer.token().getType() != BDTokenType.EOF) {
			throw new BDParseException("Extra input at end");
		}

		return statements;
	}

	/**
	 * Parses a list of assignments in an UPDATE.
	 * 
	 * @return The parsed list of assignments
	 * @throws BDParseException
	 */
	private NodeAssignmentList parseAssignmentList() throws BDParseException {
		String column;
		NodeValue value;

		if (m_tokenizer.token().getType() != BDTokenType.ID) {
			return null;
		}
		column = m_tokenizer.token().getName();
		m_tokenizer.advance();

		if (m_tokenizer.token().getType() != BDTokenType.EQ) {
			throw new BDParseException("Assignment list does not contain =");
		}
		m_tokenizer.advance();

		value = this.parseValue(false);

		if (m_tokenizer.token().getType() == BDTokenType.COMMA) {
			m_tokenizer.advance();
		}

		return new NodeAssignmentList(column, value, parseAssignmentList());
	}

	/**
	 * Parses a list of columns
	 * 
	 * @return The parsed list of columns
	 * @throws BDParseException
	 */
	private NodeColumnClause parseColumnClause(boolean allowStar)
			throws BDParseException {

		NodeColumnClause columnclause = null;

		if (m_tokenizer.token().getType() == BDTokenType.STAR && allowStar) {
			m_tokenizer.advance();
			columnclause = new NodeColumnStar();
		} else if (m_tokenizer.token().getType() == BDTokenType.ID) {
			String colname = m_tokenizer.token().getName();
			String identifier = null;
			m_tokenizer.advance();

			if (m_tokenizer.token().getType() == BDTokenType.AS) {
				m_tokenizer.advance();
				if (m_tokenizer.token().getType() != BDTokenType.ID) {
					throw new BDParseException("Identifier not following AS");
				}
				identifier = m_tokenizer.token().getName();
				m_tokenizer.advance();
			}

			NodeColumnList next = null;

			if (m_tokenizer.token().getType() == BDTokenType.COMMA) {
				m_tokenizer.advance();
				next = (NodeColumnList) parseColumnClause(false);
			}

			columnclause = new NodeColumnList(colname, identifier, next);

		} else {
			throw new BDParseException("Illegal start of column list");
		}

		return columnclause;
	}

	/**
	 * Parses a condition
	 * 
	 * @return A parsed condition
	 * @throws BDParseException
	 */
	private NodeCondition parseCondition() throws BDParseException {
		NodeConditionValue lhs = null;
		NodeCondOp condop = null;
		NodeConditionValue rhs = null;

		// Parse left-hand-side of condition 
		lhs = parseConditionValue();
		
		// Find boolean operator
		BDTokenType next = m_tokenizer.token().getType();
		if (next != BDTokenType.EQ && next != BDTokenType.NEQ
				&& next != BDTokenType.GT && next != BDTokenType.GTEQ
				&& next != BDTokenType.LT && next != BDTokenType.LTEQ
				&& next != BDTokenType.LIKE) {
			throw new BDParseException("Illegal value for boolean expression");
		}
		
		if (next == BDTokenType.EQ) {
			condop = new NodeCondOpEqual();
		} else if (next == BDTokenType.NEQ) {
			condop = new NodeCondOpNotEqual();
		} else if (next == BDTokenType.GT) {
			condop = new NodeCondOpGreaterThan();
		} else if (next == BDTokenType.GTEQ) {
			condop = new NodeCondOpGreaterThanOrEqual();
		} else if (next == BDTokenType.LT) {
			condop = new NodeCondOpLessThan();
		} else if (next == BDTokenType.LTEQ) {
			condop = new NodeCondOpLessThanOrEqual();
		} else if (next == BDTokenType.LIKE) {
			condop = new NodeCondOpLike();
		}
		m_tokenizer.advance();

		// Find value
		rhs = parseConditionValue();
		
		assert(lhs != null);
		assert(rhs != null);

		return new NodeCondition(lhs, condop, rhs);
	}
	
	/**
	 * Parses a list of conditions
	 * 
	 * @param eatWhere Whether or not to eat 'WHERE' in front of the current
	 * condition
	 * @return A parsed list of conditions
	 * @throws BDParseException
	 */
	private NodeConditionList parseConditionList(boolean eatWhere)
			throws BDParseException {

		// Check for 'FROM'
		if (eatWhere && m_tokenizer.token().getType() != BDTokenType.WHERE) {
			return null;
		}
		if (eatWhere) {
			m_tokenizer.advance();
		}

		NodeCondition condition = null;
		NodeBoolOp boolop = null;
		NodeConditionList rest = null;

		condition = parseCondition();

		// Is there another condition to parse?
		if (m_tokenizer.token().getType() == BDTokenType.AND
				|| m_tokenizer.token().getType() == BDTokenType.OR) {
			boolop = m_tokenizer.token().getType() == BDTokenType.AND ? new NodeBoolOpAnd()
					: new NodeBoolOpOr();
			m_tokenizer.advance();
			rest = parseConditionList(false);
		}

		return new NodeConditionList(condition, boolop, rest);
	}

	/**
	 * Parses a condition value - which can be either the name of a column, a
	 * string literal, or a number.
	 * 
	 * @return A parsed condition value
	 * @throws BDParseException
	 */
	private NodeConditionValue parseConditionValue() throws BDParseException {
		if (m_tokenizer.token().getType() == BDTokenType.NUMBER) {
			double value = m_tokenizer.token().getValue();
			m_tokenizer.advance();
			return new NodeConditionValueNumber(value);
		} else if (m_tokenizer.token().getType() == BDTokenType.LITERAL) {
			String literal = m_tokenizer.token().getName();
			m_tokenizer.advance();
			return new NodeConditionValueLiteral(literal);
		} else if (m_tokenizer.token().getType() == BDTokenType.ID) {
			String id = m_tokenizer.token().getName();
			m_tokenizer.advance();
			return new NodeConditionValueColumn(id);
		}
		throw new BDParseException(
				"Unexpected input, expecting either a column name, number, or literal in condition");
	}

	/**
	 * Parses a 'DELETE' expression
	 * 
	 * @return The parsed 'DELETE' expression
	 * @throws BDParseException
	 */
	private NodeStatement parseDelete() throws BDParseException {

		boolean hasStar = false;
		String table = "";
		NodeConditionList condList = null;

		if (m_tokenizer.token().getType() == BDTokenType.STAR) {
			hasStar = true;
			m_tokenizer.advance();
		}

		if (m_tokenizer.token().getType() != BDTokenType.FROM) {
			throw new BDParseException(
					"Delete statement does not have a FROM clause");
		}
		m_tokenizer.advance();

		if (m_tokenizer.token().getType() != BDTokenType.ID) {
			throw new BDParseException(
					"Delete statement does not have a FROM clause");
		}
		table = m_tokenizer.token().getName();
		m_tokenizer.advance();

		condList = parseConditionList(true);

		return new NodeDelete(hasStar, table, condList);
	}

	/**
	 * Parses a list of identifiers.
	 * 
	 * @return The parsed list of identifiers
	 */
	private NodeIdList parseIdList() {
		if (m_tokenizer.token().getType() != BDTokenType.ID) {
			return null;
		} else {
			String id = m_tokenizer.token().getName();
			m_tokenizer.advance();

			if (m_tokenizer.token().getType() == BDTokenType.COMMA) {
				m_tokenizer.advance();
			}

			NodeIdList rest = parseIdList();
			return new NodeIdList(id, rest);
		}
	}

	/**
	 * Parses an 'INSERT' expression.
	 * 
	 * @return The parsed 'ORDER BY' expression
	 * @throws BDParseException
	 */
	private NodeStatement parseInsert() throws BDParseException {

		String table = "";
		NodeIdList fields = null;
		NodeValueList values = null;

		if (m_tokenizer.token().getType() != BDTokenType.INTO) {
			throw new BDParseException(
					"Insert statement does not have INTO token");
		}
		m_tokenizer.advance();

		if (m_tokenizer.token().getType() != BDTokenType.ID) {
			throw new BDParseException(
					"Insert statement does not have INTO token");
		}
		table = m_tokenizer.token().getName();
		m_tokenizer.advance();

		if (m_tokenizer.token().getType() == BDTokenType.LEFTPAREN) {
			m_tokenizer.advance();
			fields = parseIdList();
			if (m_tokenizer.token().getType() != BDTokenType.RIGHTPAREN) {
				throw new BDParseException(
						"List of fields does not have a closing )");
			}
			m_tokenizer.advance();
		}

		if (m_tokenizer.token().getType() != BDTokenType.VALUES) {
			throw new BDParseException(
					"Insert statement does not have VALUES token");
		}
		m_tokenizer.advance();

		if (m_tokenizer.token().getType() != BDTokenType.LEFTPAREN) {
			throw new BDParseException("No value list specified");
		}
		m_tokenizer.advance();
		values = parseValueList();
		if (m_tokenizer.token().getType() != BDTokenType.RIGHTPAREN) {
			throw new BDParseException("List of values does not have a closing )");
		}
		m_tokenizer.advance();

		return new NodeInsert(table, fields, values);
	}

	/**
	 * Parses an 'ORDER BY' expression
	 * 
	 * @param eatOrderBy Whether or not to eat the 'ORDER' and 'BY' tokens
	 * @return The parsed 'ORDER BY' expression
	 * @throws BDParseException
	 */
	private NodeOrderByList parseOrderByList(boolean eatOrderBy)
			throws BDParseException {

		if (eatOrderBy) {
			if (m_tokenizer.token().getType() != BDTokenType.ORDER) {
				// throw new BDParseException("'ORDER BY' clause does not start
				// with 'ORDER'");
				return null;
			}
			m_tokenizer.advance();
			if (m_tokenizer.token().getType() != BDTokenType.BY) {
				throw new BDParseException(
						"'ORDER BY' clause does not contain 'BY'");
			}
			m_tokenizer.advance();
		}

		String name = null;
		boolean isAscending = true;
		NodeOrderByList rest = null;

		// Get column name
		if (m_tokenizer.token().getType() != BDTokenType.ID) {
			throw new BDParseException(
					"Illegal value for column name in 'ORDER BY' clause");
		}
		name = m_tokenizer.token().getName();
		m_tokenizer.advance();

		if (m_tokenizer.token().getType() == BDTokenType.ASC
				|| m_tokenizer.token().getType() == BDTokenType.DESC) {
			isAscending = m_tokenizer.token().getType() == BDTokenType.ASC;
			m_tokenizer.advance();
		}
		if (m_tokenizer.token().getType() == BDTokenType.ASC
				|| m_tokenizer.token().getType() == BDTokenType.DESC) {
			throw new BDParseException("Illegal sort order for column");
		}

		if (m_tokenizer.token().getType() == BDTokenType.COMMA) {
			m_tokenizer.advance();
			rest = parseOrderByList(false);
		}

		return new NodeOrderByList(name, isAscending, rest);
	}

	/**
	 * Parses a select statement.
	 * 
	 * @return A SQL select statement
	 * @throws BDParseException
	 */
	private NodeStatement parseSelect() throws BDParseException {

		boolean allFlag = false;
		boolean distinctFlag = false;

		if (m_tokenizer.token().getType() == BDTokenType.DISTINCT) {
			distinctFlag = true;
			m_tokenizer.advance();
		}
		if (m_tokenizer.token().getType() == BDTokenType.ALL) {
			allFlag = true;
			m_tokenizer.advance();
		}
		if (allFlag && distinctFlag) {
			throw new BDParseException("DISTINCT and ALL both specified");
		}

		NodeColumnClause columnclause = parseColumnClause(true);
		NodeTableList tablelist = parseTableList(true);
		NodeConditionList conditionlist = parseConditionList(true);
		NodeOrderByList orderbylist = parseOrderByList(true);

		return new NodeSelect(allFlag, distinctFlag, columnclause, tablelist,
				conditionlist, orderbylist);
	}

	/**
	 * Parses a table list
	 * 
	 * @param eatFrom
	 * @return
	 * @throws BDParseException
	 */
	private NodeTableList parseTableList(boolean eatFrom) throws BDParseException {

		// Eat a FROM token if starting list
		if (eatFrom) {
			if (m_tokenizer.token().getType() != BDTokenType.FROM) {
				throw new BDParseException("BDTable list does not start with FROM");
			}
			m_tokenizer.advance();
		}

		// Check to make sure a valid id starts table list
		if (m_tokenizer.token().getType() != BDTokenType.ID) {
			throw new BDParseException(
					"FROM clause does not start with an identifier");
		}

		String table = m_tokenizer.token().getName();
		String identifier = null;
		NodeTableList next = null;

		m_tokenizer.advance();

		// See if table name is qualified with an "AS <id>"
		if (m_tokenizer.token().getType() == BDTokenType.AS) {
			m_tokenizer.advance();
			if (m_tokenizer.token().getType() != BDTokenType.ID) {
				throw new BDParseException("Identifier for token " + table
						+ " is not a valid identifier");
			}
			identifier = m_tokenizer.token().getName();
			m_tokenizer.advance();
		}

		// Check for inner and outer joins 
		BDJoinType joinType = BDJoinType.NONE;
		
		if(m_tokenizer.token().getType() == BDTokenType.LEFT) {
			m_tokenizer.advance();
			if(m_tokenizer.token().getType() != BDTokenType.JOIN) {
				throw new BDParseException("No JOIN token following LEFT token!");
			}
			m_tokenizer.advance();
			joinType = BDJoinType.LEFT_OUTER;
		}
		if(m_tokenizer.token().getType() == BDTokenType.RIGHT) {
			m_tokenizer.advance();
			if(m_tokenizer.token().getType() != BDTokenType.JOIN) {
				throw new BDParseException("No JOIN token following LEFT token!");
			}
			m_tokenizer.advance();
			joinType = BDJoinType.RIGHT_OUTER;
		}
		if(m_tokenizer.token().getType() == BDTokenType.OUTER) {
			m_tokenizer.advance();
			if(m_tokenizer.token().getType() != BDTokenType.JOIN) {
				throw new BDParseException("No JOIN token following LEFT token!");
			}
			m_tokenizer.advance();
			joinType = BDJoinType.FULL_OUTER;
		}
		if(m_tokenizer.token().getType() == BDTokenType.INNER) {
			m_tokenizer.advance();
			if(m_tokenizer.token().getType() != BDTokenType.JOIN) {
				throw new BDParseException("No JOIN token following LEFT token!");
			}
			m_tokenizer.advance();
			joinType = BDJoinType.INNER;
		}
		
		// Parse rest of table list
		if (m_tokenizer.token().getType() == BDTokenType.COMMA) {
			m_tokenizer.advance();
			next = (NodeTableList) parseTableList(false);
		} else if (joinType != BDJoinType.NONE) {
			next = (NodeTableList) parseTableList(false);
		}
		
		return new NodeTableList(table, identifier, joinType, next);
	}

	/**
	 * Parses an 'UPDATE' expression.
	 * 
	 * @return The parsed 'UPDATE' expression
	 * @throws BDParseException
	 */
	private NodeStatement parseUpdate() throws BDParseException {
		String table = "";
		NodeAssignmentList assignments = null;
		NodeConditionList conditions = null;

		// Check to make sure a column name is specified
		if (m_tokenizer.token().getType() != BDTokenType.ID) {
			throw new BDParseException(
					"Column name is not specified in UPDATE statement");
		}
		table = m_tokenizer.token().getName();
		m_tokenizer.advance();

		if (m_tokenizer.token().getType() != BDTokenType.SET) {
			throw new BDParseException("UPDATE statement does not include 'SET'");
		}
		m_tokenizer.advance();

		assignments = parseAssignmentList();
		conditions = parseConditionList(true);

		return new NodeUpdate(table, assignments, conditions);
	}

	/**
	 * Parses a value (either a string literal or a number)
	 * 
	 * @param isLiteral Whether or not the value has to be a literal
	 * @return The parsed value
	 * @throws BDParseException
	 */
	private NodeValue parseValue(boolean isLiteral) throws BDParseException {

		NodeValue value = null;

		if (m_tokenizer.token().getType() == BDTokenType.LITERAL) {
			value = new NodeValueLiteral(m_tokenizer.token().getName());
			m_tokenizer.advance();
		} else if (m_tokenizer.token().getType() == BDTokenType.NUMBER
				&& !isLiteral) {
			value = new NodeValueNumber(m_tokenizer.token().getValue());
			m_tokenizer.advance();
		} else {
			throw new BDParseException("Illegal type for value");
		}

		return value;
	}

	/**
	 * Parses a list of values
	 * 
	 * @return The parsed list of values
	 */
	private NodeValueList parseValueList() {
		NodeValue value = null;
		NodeValueList rest = null;

		try {
			value = parseValue(false);
		} catch (BDParseException e) {
			return null;
		}

		if (m_tokenizer.token().getType() == BDTokenType.COMMA) {
			m_tokenizer.advance();
			rest = parseValueList();
		}

		return new NodeValueList(value, rest);
	}
}
