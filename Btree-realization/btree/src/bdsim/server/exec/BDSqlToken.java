package bdsim.server.exec;

/**
 * Represents a single token in the SQL input. The <code>BDSqlTokenizer</code>
 * will break the input up into these single tokens for easy parsing.
 * 
 * @author wpijewsk
 * @revision $Id: BDSqlToken.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public class BDSqlToken {

    private String m_name;
    private double m_value;
    private BDTokenType m_type;

    /**
     * Class constructor.
     * 
     * @param type The <code>BDTokenType</code> of this token
     */
    public BDSqlToken(BDTokenType type) {
        this(type, null, -1);
    }

    /**
     * Class constructor specifying name.
     * 
     * @param type The <code>BDTokenType</code> of this token
     * @param name The name associated with this token
     */
    public BDSqlToken(BDTokenType type, String name) {
        this(type, name, -1);
    }

    /**
     * Class constructor specifying value.
     * 
     * @param type The <code>BDTokenType</code> of this token
     * @param field The value associated with this token
     */
    public BDSqlToken(BDTokenType type, double value) {
        this(type, null, value);
    }

    /**
     * Constructor for <code>BDSqlToken</code> class. Hidden since a token
     * cannot have both a name and a value.
     * 
     * @param type
     * @param name
     * @param value
     */
    private BDSqlToken(BDTokenType type, String name, double value) {
        this.m_name = name;
        this.m_value = value;
        this.m_type = type;
    }

    /**
     * 
     * @return The <code>BDTokenType</code> of this token
     */
    public BDTokenType getType() {
        return m_type;
    }

    public String getName() {
        return m_name;
    }

    public double getValue() {
        return m_value;
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object other) {
        if ((other == null) || !(other instanceof BDSqlToken)) {
            return false;
        }

        BDSqlToken otherToken = (BDSqlToken) other;

        boolean areNamesEqual;
        if (this.m_name == null) {
            areNamesEqual = (otherToken.m_name == null);
        } else {
            areNamesEqual = this.m_name.equals(otherToken.m_name);
        }

        boolean areValuesEqual = this.m_value == otherToken.m_value;
        boolean areTypesEqual = this.m_type == otherToken.m_type;

        return areTypesEqual && areNamesEqual && areValuesEqual;
    }
}
