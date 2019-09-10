package bdsim.common;

/**
 * Represents a request to the server from the client.
 * 
 * @author wpijewsk
 * @revision $Id: BDRequest.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public interface BDRequest { 
	public BDResponse handle(BDRequestHandler handler);
}
