package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;
import bdsim.common.BDResultSet;

/**
 * A response from the server with results. 
 * 
 * @author wpijewsk
 * @revision $Id: BDResultResponse.java 190 2006-05-14 20:39:29 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDResultResponse implements BDResponse, Serializable {

	private static final long serialVersionUID = 1613512129923224644L;

	private BDResultSet m_results;

	public BDResultResponse(BDResultSet results) {
		this.m_results = results;
	}

	public BDResultSet getResults() {
		return m_results;
	}

	public void handle(BDResponseHandler handler, long elapsed) {
		handler.handle(this, elapsed);
	}
}
