package bdsim.common.messages;

import java.io.Serializable;

import org.w3c.dom.Document;

import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;

/**
 * @author wpijewsk
 * @revision $Id: BDSaveResponse.java 192 2006-05-14 21:57:53 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDSaveResponse implements BDResponse, Serializable {

	private static final long serialVersionUID = -3420584319050485107L;
	private Document m_doc;

	/**
	 * Class constructor.
	 * 
	 * @param doc
	 */
	public BDSaveResponse(Document doc) {
		this.m_doc = doc;
	}

	public Document getDoc() {
		return m_doc;
	}

	public void handle(BDResponseHandler handler, long elapsed) {
		handler.handle(this, elapsed);
	}
}
