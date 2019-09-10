package bdsim.common.messages;

import java.io.Serializable;
import java.util.Map;

import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;
import bdsim.server.system.BDSchema;

/**
 * @author wpijewsk
 * @revision $Id: BDInfoResponse.java 190 2006-05-14 20:39:29 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDInfoResponse implements Serializable, BDResponse {

	private static final long serialVersionUID = -2486150595525473881L;
	private String m_fileName;
	private Map<String, BDSchema> m_schemas;

	/**
	 * Class constructor.
	 * 
	 * @param m_schemas
	 */
	public BDInfoResponse(Map<String, BDSchema> schemas, String fileName) {
		this.m_schemas = schemas;
		this.m_fileName = fileName;
	}

	public String getFileName() {
		return m_fileName;
	}

	public Map<String, BDSchema> getSchemas() {
		return m_schemas;
	}

	public void handle(BDResponseHandler handler, long elapsed) {
		handler.handle(this, elapsed);
	}
}
