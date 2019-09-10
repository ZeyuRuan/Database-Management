package bdsim.common;

import bdsim.common.messages.BDCreateRequest;
import bdsim.common.messages.BDDropRequest;
import bdsim.common.messages.BDInfoRequest;
import bdsim.common.messages.BDLoadRequest;
import bdsim.common.messages.BDSaveRequest;
import bdsim.common.messages.BDSqlRequest;


/**
 * Specifies all the different types of requests you can handle.
 * 
 * @author wpijewsk
 * @revision $Id: BDRequestHandler.java 191 2006-05-14 21:22:58 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public interface BDRequestHandler {
	public BDResponse handle(BDInfoRequest request);
	public BDResponse handle(BDSqlRequest request);
	public BDResponse handle(BDCreateRequest request);
	public BDResponse handle(BDDropRequest request);
	public BDResponse handle(BDLoadRequest request);
	public BDResponse handle(BDSaveRequest request);
}
