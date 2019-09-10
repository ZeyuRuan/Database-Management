package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;

/**
 * A response from the database when a table is created.
 * 
 * @author wpijewsk
 * @revision $Id: BDCreateResponse.java 190 2006-05-14 20:39:29 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDCreateResponse implements BDResponse, Serializable {

	private static final long serialVersionUID = 5931559904247490053L;
	private String m_tableName;

	/**
	 * Class constructor.
	 * 
	 * @param name
	 */
	public BDCreateResponse(String name) {
		m_tableName = name;
	}

	public String getTableName() {
		return m_tableName;
	}

	public void handle(BDResponseHandler handler, long elapsed) {
		handler.handle(this, elapsed);
	}
}
