package bdsim.server.exec;

import junit.framework.TestCase;

public class SqlTokenTest extends TestCase {
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }    
    
    public void testEquality() {
        BDSqlToken token1 = new BDSqlToken(BDTokenType.NUMBER, 413.23);
        BDSqlToken token2 = new BDSqlToken(BDTokenType.NUMBER, 413.23);
        assertEquals(token1, token2);
        
        token1 = new BDSqlToken(BDTokenType.ID, "name1");
        token2 = new BDSqlToken(BDTokenType.ID, "name1");
        assertEquals(token1, token2);    
        
        token1 = new BDSqlToken(BDTokenType.ID, "name1x");
        token2 = new BDSqlToken(BDTokenType.ID, "name1");
        assertFalse(token1.equals(token2));   
        
        token1 = new BDSqlToken(BDTokenType.GT);
        token2 = new BDSqlToken(BDTokenType.DESC);
        assertFalse(token1.equals(token2)); 
        
        token1 = new BDSqlToken(BDTokenType.NUMBER, 431.23);
        token2 = new BDSqlToken(BDTokenType.NUMBER, 431.22);
        assertFalse(token1.equals(token2));     
        
        token1 = new BDSqlToken(BDTokenType.NUMBER, 431.23);
        token2 = new BDSqlToken(BDTokenType.NUMBER, "name1");
        assertFalse(token1.equals(token2));           
    }

    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }    
}
