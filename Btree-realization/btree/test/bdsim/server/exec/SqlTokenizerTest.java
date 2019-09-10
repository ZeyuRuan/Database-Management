package bdsim.server.exec;

import junit.framework.TestCase;

/**
 * Unit tests for the <code>cs127db.server.sql.SqlTokenizer</code> class.
 * 
 * @author wpijewsk
 */
public class SqlTokenizerTest extends TestCase {

    private BDSqlTokenizer m_tokenizer;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testSimple() {
        m_tokenizer = new BDSqlTokenizer("SELECT *");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.STAR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());

        m_tokenizer = new BDSqlTokenizer("SELECT * FROM");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.STAR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
    }

    public void testCapitalization() {
        m_tokenizer = new BDSqlTokenizer("sElEcT * from WHERE Like");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.STAR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.WHERE, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.LIKE, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
    }
    
    public void testKeywords() {
        m_tokenizer = new BDSqlTokenizer("all and as asc by delete desc distinct = from group > >= insert like < <= <> or order select * update where values into set");
        m_tokenizer.advance();
        assertEquals(BDTokenType.ALL, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.AND, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.AS, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ASC, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.BY, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.DELETE, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.DESC, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.DISTINCT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EQ, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.GROUP, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.GT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.GTEQ , m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.INSERT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.LIKE, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.LT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.LTEQ, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.NEQ , m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.OR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ORDER, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.STAR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.UPDATE, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.WHERE, m_tokenizer.token().getType());        
        m_tokenizer.advance();
        assertEquals(BDTokenType.VALUES, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.INTO, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.SET, m_tokenizer.token().getType());
    }
    

    public void testComma1() {
        m_tokenizer = new BDSqlTokenizer("SELECT , * , FROM");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.STAR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
    }

    public void testComma2() {
        m_tokenizer = new BDSqlTokenizer("SELECT,*,FROM");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.STAR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
    }

    public void testComma3() {
        m_tokenizer = new BDSqlTokenizer(
                "select col1, col2    ,col3,col4 from table1,table2,    table3");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col1", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col2", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col3", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col4", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("table1", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("table2", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("table3", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
    }

    public void testComma4() {
        m_tokenizer = new BDSqlTokenizer(",,,select, ,  ");
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
    }

    public void testId() {
        m_tokenizer = new BDSqlTokenizer("select distinct col1 from table1");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.DISTINCT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col1", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("table1", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
    }

    public void testLiteral1() {
        m_tokenizer = new BDSqlTokenizer(
                "select * from table1 where col1 = 'Bill'");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.STAR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("table1", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.WHERE, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col1", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EQ, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.LITERAL, m_tokenizer.token().getType());
        assertEquals("Bill", m_tokenizer.token().getName());
    }

    public void testLiteral2() {
        m_tokenizer = new BDSqlTokenizer(
                "select * from table1 where col1 = 'Bill' and col2 like '%part%' or col3 > 32.1");
        m_tokenizer.advance();
        assertEquals(BDTokenType.SELECT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.STAR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.FROM, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("table1", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.WHERE, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col1", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.EQ, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.LITERAL, m_tokenizer.token().getType());
        assertEquals("Bill", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.AND, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col2", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.LIKE, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.LITERAL, m_tokenizer.token().getType());
        assertEquals("%part%", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.OR, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("col3", m_tokenizer.token().getName());
        m_tokenizer.advance();
        assertEquals(BDTokenType.GT, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.NUMBER, m_tokenizer.token().getType());
        assertEquals(32.1, m_tokenizer.token().getValue());
		m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());                        
    }
    
    /**
     * Tests tokenizing parentheses.
     */
    public void testParen() {
		m_tokenizer = new BDSqlTokenizer(
				"INSERT INTO members (column1, column2) VALUES (value1)");
		m_tokenizer.advance();
        assertEquals(BDTokenType.INSERT, m_tokenizer.token().getType());
		m_tokenizer.advance();
        assertEquals(BDTokenType.INTO, m_tokenizer.token().getType());   
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("members", m_tokenizer.token().getName());    
		m_tokenizer.advance();
        assertEquals(BDTokenType.LEFTPAREN, m_tokenizer.token().getType());
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("column1", m_tokenizer.token().getName());   
        m_tokenizer.advance();
        assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());   
        m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
        assertEquals("column2", m_tokenizer.token().getName());   
		m_tokenizer.advance();
        assertEquals(BDTokenType.RIGHTPAREN, m_tokenizer.token().getType());        
		m_tokenizer.advance();
        assertEquals(BDTokenType.VALUES, m_tokenizer.token().getType());  
		m_tokenizer.advance();
        assertEquals(BDTokenType.LEFTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
        assertEquals(BDTokenType.ID, m_tokenizer.token().getType());   
        assertEquals("value1", m_tokenizer.token().getName());     
		m_tokenizer.advance();
        assertEquals(BDTokenType.RIGHTPAREN, m_tokenizer.token().getType());   
		m_tokenizer.advance();
        assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());    
        
        
		m_tokenizer = new BDSqlTokenizer(
				"INSERT INTO members(     column1   ,column2) VALUES (    value1)");
		m_tokenizer.advance();
		assertEquals(BDTokenType.INSERT, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.INTO, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
		assertEquals("members", m_tokenizer.token().getName());
		m_tokenizer.advance();
		assertEquals(BDTokenType.LEFTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
		assertEquals("column1", m_tokenizer.token().getName());
		m_tokenizer.advance();
		assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
		assertEquals("column2", m_tokenizer.token().getName());
		m_tokenizer.advance();
		assertEquals(BDTokenType.RIGHTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.VALUES, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.LEFTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
		assertEquals("value1", m_tokenizer.token().getName());
		m_tokenizer.advance();
		assertEquals(BDTokenType.RIGHTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
	}

	public void testTransaction() {
		m_tokenizer = new BDSqlTokenizer(
				"BEGIN    TRANSACTION  INSERT INTO members(     column1   ,column2) VALUES (    value1) COMMIT TRANSACTION");
		m_tokenizer.advance();
		assertEquals(BDTokenType.BEGIN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.TRANSACTION, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.INSERT, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.INTO, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
		assertEquals("members", m_tokenizer.token().getName());
		m_tokenizer.advance();
		assertEquals(BDTokenType.LEFTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
		assertEquals("column1", m_tokenizer.token().getName());
		m_tokenizer.advance();
		assertEquals(BDTokenType.COMMA, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
		assertEquals("column2", m_tokenizer.token().getName());
		m_tokenizer.advance();
		assertEquals(BDTokenType.RIGHTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.VALUES, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.LEFTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.ID, m_tokenizer.token().getType());
		assertEquals("value1", m_tokenizer.token().getName());
		m_tokenizer.advance();
		assertEquals(BDTokenType.RIGHTPAREN, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.COMMIT, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.TRANSACTION, m_tokenizer.token().getType());
		m_tokenizer.advance();
		assertEquals(BDTokenType.EOF, m_tokenizer.token().getType());
	}

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        m_tokenizer = null;
    }
}
