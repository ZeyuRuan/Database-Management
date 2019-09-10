package bdsim.common;

import bdsim.common.messages.BDCreateResponse;
import bdsim.common.messages.BDDropResponse;
import bdsim.common.messages.BDErrorResponse;
import bdsim.common.messages.BDInfoResponse;
import bdsim.common.messages.BDLoadResponse;
import bdsim.common.messages.BDResultResponse;
import bdsim.common.messages.BDSaveResponse;

/**
 * @author wpijewsk
 * @revision $Id: BDResponseHandler.java 191 2006-05-14 21:22:58 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public interface BDResponseHandler {
	public void handle(BDCreateResponse response, long elapsed);
	public void handle(BDDropResponse response, long elapsed);
	public void handle(BDErrorResponse response, long elapsed);
	public void handle(BDInfoResponse response, long elapsed);
	public void handle(BDLoadResponse response, long elapsed);
	public void handle(BDResultResponse response, long elapsed);
	public void handle(BDSaveResponse response, long elapsed);
}
