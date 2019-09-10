package bdsim.common.messages;

import java.io.Serializable;

import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;

/**
 * @author wpijewsk
 * @revision $Id: BDLoadResponse.java 191 2006-05-14 21:22:58 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDLoadResponse implements Serializable, BDResponse {

	private static final long serialVersionUID = 6914512685991664898L;

	public void handle(BDResponseHandler handler, long elapsed) {
		handler.handle(this, elapsed);
	}
}
