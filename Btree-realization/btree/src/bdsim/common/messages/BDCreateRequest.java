package bdsim.common.messages;

import java.io.Serializable;
import java.util.Vector;

import bdsim.common.BDRequest;
import bdsim.common.BDRequestHandler;
import bdsim.common.BDResponse;
import bdsim.server.system.BDObjectType;

/**
 * A request to create a table.
 * 
 * @author wpijewsk
 * @revision $Id: BDCreateRequest.java 190 2006-05-14 20:39:29 +0000 (Sun, 14 May 2006) wpijewsk $
 */
public final class BDCreateRequest implements BDRequest, Serializable {

	private static final long serialVersionUID = -1313005955246653660L;
	private Vector<String> m_names;
	private String m_primaryKey;
	private String m_tableName;
	private Vector<BDObjectType> m_types;

	/**
	 * Class constructor.
	 * 
	 * @param tableName
	 * @param names
	 * @param types
	 * @param key
	 */
	public BDCreateRequest(String tableName, Vector<String> names,
			Vector<BDObjectType> types, String key) {
		this.m_tableName = tableName;
		this.m_names = names;
		this.m_types = types;
		this.m_primaryKey = key;
	}

	public Vector<String> getNames() {
		return m_names;
	}

	public String getPrimaryKey() {
		return m_primaryKey;
	}

	public String getTableName() {
		return m_tableName;
	}

	public Vector<BDObjectType> getTypes() {
		return m_types;
	}

	public BDResponse handle(BDRequestHandler handler) {
		return handler.handle(this);
	}
}
