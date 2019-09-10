package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDRequest;
import bdsim.common.BDRequestHandler;
import bdsim.common.BDResponse;

/**
 * @author wpijewsk
 * @revision $Id: BDSqlRequest.java 190 2006-05-14 20:39:29 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDSqlRequest implements BDRequest, Serializable {

	private static final long serialVersionUID = 295408025484005578L;
	private String m_sql;
	
	public BDSqlRequest(String sql) {
		this.m_sql = sql;
	}

    /**
     * @return The SQL in this request;
     */
    public String getSql() {
        return m_sql;
    }

	public BDResponse handle(BDRequestHandler handler) {
		return handler.handle(this);
	}
}
