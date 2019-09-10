package bdsim.common.messages;

import java.io.Serializable;

import org.w3c.dom.Document;

import bdsim.common.BDRequest;
import bdsim.common.BDRequestHandler;
import bdsim.common.BDResponse;

/**
 * @author wpijewsk
 * @revision $Id: BDLoadRequest.java 191 2006-05-14 21:22:58 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDLoadRequest implements BDRequest, Serializable {

	private static final long serialVersionUID = 6327664924615129417L;
	private Document m_document;
	private String m_fileName;
	
	/**
	 * Class constructor.
	 * 
	 * @param document
	 * @param name
	 */
	public BDLoadRequest(Document document, String name) {
		this.m_document = document;
		m_fileName = name;
	}
	
	public Document getDocument() {
		return m_document;
	}

	public String getFileName() {
		return m_fileName;
	}

	public BDResponse handle(BDRequestHandler handler) {
		return handler.handle(this);
	}
}
