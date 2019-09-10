package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDRequest;
import bdsim.common.BDRequestHandler;
import bdsim.common.BDResponse;

/**
 * Represents a request for information about the schema of the database.
 * 
 * @author wpijewsk
 * @revision $Id: BDInfoRequest.java 190 2006-05-14 20:39:29 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDInfoRequest implements BDRequest, Serializable {

	private static final long serialVersionUID = -7034226796336026836L;

	public BDInfoRequest() {
	}

	public BDResponse handle(BDRequestHandler handler) {
		return handler.handle(this);
	}
}
