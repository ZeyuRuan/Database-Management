package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;

/**
 * @author wpijewsk
 * @revision $Id: BDErrorResponse.java 190 2006-05-14 20:39:29 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDErrorResponse implements BDResponse, Serializable {

	private static final long serialVersionUID = -3957031360110130127L;
	private String m_message;
	
	/**
	 * Class constructor.
	 * 
	 * @param message
	 */
	public BDErrorResponse(String message) {
		this.m_message = message;
	}

	public String getMessage() {
		return m_message;
	}

	public void handle(BDResponseHandler handler, long elapsed) {
		handler.handle(this, elapsed);
	}
}
