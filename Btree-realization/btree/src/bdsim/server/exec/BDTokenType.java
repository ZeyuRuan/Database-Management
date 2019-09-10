package bdsim.server.exec;

/**
 * Represents all of the possible SQL tokens.
 * 
 * @author wpijewsk
 * @revision $Id: BDTokenType.java 183 2006-05-14 02:33:56 +0000 (Sun, 14 May 2006) wpijewsk $
 */
enum BDTokenType {
	ALL, AND, AS, ASC, BEGIN, BY, COMMA, COMMIT, DELETE, DESC, DISTINCT, EOF, EQ,
	FROM, GROUP, GT, GTEQ, ID, INNER, INSERT, INTO, JOIN, LEFT, LEFTPAREN,
	LIKE, LITERAL, LT, LTEQ, NEQ, NUMBER, ON, OR, ORDER, OUTER, RIGHT,
	RIGHTPAREN, ROLLBACK, SELECT, SET, STAR, TRANSACTION, UPDATE, VALUES, WHERE;
}