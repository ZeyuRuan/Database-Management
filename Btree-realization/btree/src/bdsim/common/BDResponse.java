package bdsim.common;

/**
 * @author wpijewsk
 * @revision $Id: BDResponse.java 200 2006-05-15 21:00:11 +0000 (Mon, 15 May 2006) dclee $
 */
public interface BDResponse {
	public void handle(BDResponseHandler handler, long elapsed);
}
