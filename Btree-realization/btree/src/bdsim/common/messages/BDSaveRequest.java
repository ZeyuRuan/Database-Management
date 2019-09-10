package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDRequest;
import bdsim.common.BDRequestHandler;
import bdsim.common.BDResponse;

/**
 * @author wpijewsk
 * @revision $Id: BDSaveRequest.java 192 2006-05-14 21:57:53 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDSaveRequest implements BDRequest, Serializable {

	private static final long serialVersionUID = 1265341041273769800L;

	public BDResponse handle(BDRequestHandler handler) {
		return handler.handle(this);
	}
}
