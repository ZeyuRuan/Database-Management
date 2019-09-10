package bdsim.server.exec;

/**
 * Tokenizes SQL input into a sequence of <code>SqlTokens</code>.
 * 
 * @author wpijewsk
 * @revision $Id: BDSqlTokenizer.java 183 2006-05-14 02:33:56 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDSqlTokenizer {

	private String m_text;
	private int m_curloc;
	private BDSqlToken m_token;

	/**
	 * Class constructor.
	 * @param text  The text to tokenize
	 */
	public BDSqlTokenizer(String text) {
		this.m_text = text;
		this.m_curloc = 0;
		m_text.trim();
	}

	/**
	 * @return The next token in the sequence of tokens.
	 */
	public BDSqlToken token() {
		return m_token;
	}

	/**
	 * Advances the tokenizer and stores that new token.   
	 */
	public void advance() {

		// Handle end-of-string case
		if (m_curloc >= m_text.length()) {
			m_token = new BDSqlToken(BDTokenType.EOF);
			return;
		}

		// Eat up any leading whitespace
		while (m_text.charAt(m_curloc) == ' ') {
			m_curloc++;
			if (m_curloc >= m_text.length()) {
				m_token = new BDSqlToken(BDTokenType.EOF);
				return;
			}
		}

		// Are we dealing with a comma?
		if (m_text.charAt(m_curloc) == ',') {
			m_curloc++;
			m_token = new BDSqlToken(BDTokenType.COMMA);
			return;
		}
		// Are we dealing with a right parenthesis?
		else if (m_text.charAt(m_curloc) == '(') {
			m_curloc++;
			m_token = new BDSqlToken(BDTokenType.LEFTPAREN);
			return;
		}		
		// Are we dealing with a left parenthesis?
		else if (m_text.charAt(m_curloc) == ')') {
			m_curloc++;
			m_token = new BDSqlToken(BDTokenType.RIGHTPAREN);
			return;
		}		
		// Are we dealing with a left parenthesis?
		else if (m_text.charAt(m_curloc) == '=') {
			m_curloc++;
			m_token = new BDSqlToken(BDTokenType.EQ);
			return;
		}			

		// Handle normal tokens
		if (m_curloc >= m_text.length() || m_text.charAt(m_curloc) == ' '
				|| m_text.charAt(m_curloc) == ','
				|| m_text.charAt(m_curloc) == '=') {
			try {
				throw new BDParseException("Internal exception");
			} catch (BDParseException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		int start = m_curloc;
		int end = m_curloc;

		char cur = m_text.charAt(end);
        
		String next = null;
		String lowerNext = null;        
		
        // FIXME I need to make sure this works.
        if (cur == '\'') {
            end++;
            cur = m_text.charAt(end);
			while (end < m_text.length() && cur != '\'') {
				end++;

				if (end == m_text.length()) {
					break;
				}
				cur = m_text.charAt(end);
			}
    		next = m_text.substring(start, end);
    		lowerNext = next.toLowerCase();                  
            end++;
		} else if (cur == '"') {
            end++;
            cur = m_text.charAt(end);
			while (end < m_text.length() && cur != '"') {
				end++;

				if (end == m_text.length()) {
					break;
				}
				cur = m_text.charAt(end);
			}
    		next = m_text.substring(start, end);
    		lowerNext = next.toLowerCase();   
            end++;
		} else {
			while (end < m_text.length() && cur != ' ' && cur != ','
					&& cur != '(' && cur != ')') {
				end++;

				if (end == m_text.length()) {
					break;
				}
				cur = m_text.charAt(end);
			}
    		next = m_text.substring(start, end);
    		lowerNext = next.toLowerCase();             
		}
       


		// Check to see if a keyword
		if (lowerNext.equals("select")) {
			m_token = new BDSqlToken(BDTokenType.SELECT);
		} else if (lowerNext.equals("asc")) {
			m_token = new BDSqlToken(BDTokenType.ASC);
		} else if (lowerNext.equals("desc")) {
			m_token = new BDSqlToken(BDTokenType.DESC);
		} else if (lowerNext.equals("*")) {
			m_token = new BDSqlToken(BDTokenType.STAR);
		} else if (lowerNext.equals("from")) {
			m_token = new BDSqlToken(BDTokenType.FROM);
		} else if (lowerNext.equals("where")) {
			m_token = new BDSqlToken(BDTokenType.WHERE);
		} else if (lowerNext.equals("and")) {
			m_token = new BDSqlToken(BDTokenType.AND);
		} else if (lowerNext.equals("or")) {
			m_token = new BDSqlToken(BDTokenType.OR);
		} else if (lowerNext.equals("like")) {
			m_token = new BDSqlToken(BDTokenType.LIKE);
		} else if (lowerNext.equals("all")) {
			m_token = new BDSqlToken(BDTokenType.ALL);
		} else if (lowerNext.equals("distinct")) {
			m_token = new BDSqlToken(BDTokenType.DISTINCT);
		} else if (lowerNext.equals("insert")) {
			m_token = new BDSqlToken(BDTokenType.INSERT);
		} else if (lowerNext.equals("delete")) {
			m_token = new BDSqlToken(BDTokenType.DELETE);
		} else if (lowerNext.equals("update")) {
			m_token = new BDSqlToken(BDTokenType.UPDATE);
		} else if (lowerNext.equals("order")) {
			m_token = new BDSqlToken(BDTokenType.ORDER);
		} else if (lowerNext.equals("group")) {
			m_token = new BDSqlToken(BDTokenType.GROUP);
		} else if (lowerNext.equals("by")) {
			m_token = new BDSqlToken(BDTokenType.BY);
		} else if (lowerNext.equals("<>")) {
			m_token = new BDSqlToken(BDTokenType.NEQ);
		} else if (lowerNext.equals(">")) {
			m_token = new BDSqlToken(BDTokenType.GT);
		} else if (lowerNext.equals("<")) {
			m_token = new BDSqlToken(BDTokenType.LT);
		} else if (lowerNext.equals(">=")) {
			m_token = new BDSqlToken(BDTokenType.GTEQ);
		} else if (lowerNext.equals("<=")) {
			m_token = new BDSqlToken(BDTokenType.LTEQ);
		} else if (lowerNext.equals("as")) {
			m_token = new BDSqlToken(BDTokenType.AS);
		} else if (lowerNext.equals("values")) {
			m_token = new BDSqlToken(BDTokenType.VALUES);
		} else if (lowerNext.equals("into")) {
			m_token = new BDSqlToken(BDTokenType.INTO);
		} else if (lowerNext.equals("set")) {
            m_token = new BDSqlToken(BDTokenType.SET);
        } else if (lowerNext.equals("inner")) {
            m_token = new BDSqlToken(BDTokenType.INNER);
        } else if (lowerNext.equals("join")) {
            m_token = new BDSqlToken(BDTokenType.JOIN);
        } else if (lowerNext.equals("right")) {
            m_token = new BDSqlToken(BDTokenType.RIGHT);
        } else if (lowerNext.equals("left")) {
            m_token = new BDSqlToken(BDTokenType.LEFT);
        } else if (lowerNext.equals("outer")) {
            m_token = new BDSqlToken(BDTokenType.OUTER);
        } else if (lowerNext.equals("on")) {
            m_token = new BDSqlToken(BDTokenType.ON);
        } else if (lowerNext.equals("begin")) {
            m_token = new BDSqlToken(BDTokenType.BEGIN);
        } else if (lowerNext.equals("commit")) {
            m_token = new BDSqlToken(BDTokenType.COMMIT);
        } else if (lowerNext.equals("rollback")) {
            m_token = new BDSqlToken(BDTokenType.ROLLBACK);
        } else if (lowerNext.equals("transaction")) {
            m_token = new BDSqlToken(BDTokenType.TRANSACTION);
        } else {
            // If not a keyword, must wither be identifier or number
            boolean isNumeric = true;
			double value = -1;
			try {
				value = Double.valueOf(next).doubleValue();
			} catch (NumberFormatException e) {
				isNumeric = false;
			}

			if (isNumeric) {
				m_token = new BDSqlToken(BDTokenType.NUMBER, value);
			} else {
				if (next.startsWith("'") || next.startsWith("\"")) {
					m_token = new BDSqlToken(BDTokenType.LITERAL, next.substring(1,
							next.length() ));
				} else {
					if (0 <= next.indexOf("=")) {
						end = start + next.indexOf("=");
						next = next.substring(0, next.indexOf("="));
					}
					
					m_token = new BDSqlToken(BDTokenType.ID, next);
				}
			}
		}
		m_curloc = end;
	}
}
