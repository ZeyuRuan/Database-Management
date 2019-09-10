package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;

/**
 * @author wpijewsk
 */
public final class BDDropResponse implements Serializable, BDResponse {

	private static final long serialVersionUID = -5340118050794211752L;
	private String m_tableName;

	/**
	 * Class constructor.
	 * 
	 * @param name
	 */
	public BDDropResponse(String name) {
		m_tableName = name;
	}

	public String getTableName() {
		return m_tableName;
	}

	public void handle(BDResponseHandler handler, long elapsed) {
		handler.handle(this, elapsed);
	}
}
