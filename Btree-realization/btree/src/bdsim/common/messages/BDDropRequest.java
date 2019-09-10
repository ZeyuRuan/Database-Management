package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDRequest;
import bdsim.common.BDRequestHandler;
import bdsim.common.BDResponse;

/**
 * A request to drop a table from the database.
 * 
 * @author wpijewsk
 * @revision $Id: BDDropRequest.java 190 2006-05-14 20:39:29 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDDropRequest implements BDRequest, Serializable {

	private static final long serialVersionUID = -4007483459703481540L;
	private String m_tableName;

	/**
	 * Class constructor.
	 * 
	 * @param name
	 */
	public BDDropRequest(String name) {
		m_tableName = name;
	}

	public String getTableName() {
		return m_tableName;
	}

	public BDResponse handle(BDRequestHandler handler) {
		return handler.handle(this);
	}
}
