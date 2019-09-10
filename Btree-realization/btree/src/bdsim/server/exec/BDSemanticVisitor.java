package bdsim.server.exec;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import bdsim.server.exec.nodes.BDCondOpType;
import bdsim.server.exec.nodes.BDJoinType;
import bdsim.server.exec.nodes.NodeCondition;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDTable;

/**
 * Visits each node in the abstract syntax tree to make sure that elements of
 * the syntax tree that refer to schema elements (names of tables and columns)
 * are valid.
 * 
 * @author wpijewsk
 * @revision $Id: BDSemanticVisitor.java 235 2007-01-18 19:02:15 +0000 (Thu, 18 Jan 2007) wpijewsk $
 */
public final class BDSemanticVisitor implements BDVisitor {
    
	/**
	 * Stores the final table and column name mappings.
	 * 
	 * @author wpijewsk
	 */
	public final class BDNameMappings {
		
		/**
		 * A list of column abbreviations mapped to full column names.
		 */
        private Map<String, BDTableColumnPair> m_colAbbrs;
        
        /**
         * A list of table abbreviations mapped to tables.
         */
        private Map<String, String> m_tblAbbrs;
		
        /**
		 * @param colAbbrs
		 * @param tblAbbrs
		 */
		public BDNameMappings(Map<String, BDTableColumnPair>colAbbrs, Map<String, String> tblAbbrs) {
			this.m_colAbbrs = colAbbrs;
			this.m_tblAbbrs = tblAbbrs;
		}

		public boolean equals(Object other) {
			if(!(other instanceof BDNameMappings)) {
				return false;
			}
			BDNameMappings otherMappings = (BDNameMappings) other;
			return this.m_colAbbrs.equals(otherMappings.m_colAbbrs)
					&& this.m_tblAbbrs.equals(otherMappings.m_tblAbbrs);
		}

		public Map<String, BDTableColumnPair> getColAbbrs() {
			return m_colAbbrs;
		}   
		
		public Map<String, String> getTblAbbrs() {
			return m_tblAbbrs;
		}
    }
	
	public final class SemanticException extends Exception {

		private static final long serialVersionUID = 6973890130503805434L;

		public SemanticException(String string) {
			super(string);
		}
	}
	
    static Logger logger = Logger.getLogger(BDSemanticVisitor.class);
    
    private Map<String, BDTableColumnPair> m_colAbbrs;
    private List<String> m_colAsgns;
    private List<String> m_colCondVals;
    private Map<String, String> m_colDecls;
    private Map<String, String> m_tblAbbrs;
    
    /**
     * Class constructor.
     */
	public BDSemanticVisitor() {
        m_colAbbrs = new HashMap<String, BDTableColumnPair>();
        m_tblAbbrs = new HashMap<String, String>();
        m_colDecls = new HashMap<String, String>();
        m_colCondVals = new LinkedList<String>();
        m_colAsgns = new LinkedList<String>();
	}
    
    public BDNameMappings getMappings() {
        return new BDNameMappings(this.m_colAbbrs, this.m_tblAbbrs);
    }

    public void handleAssignment(String column, Object value) {
        m_colAsgns.add(column);
    }
    
	public void handleBoolOp(boolean isAnd) {}

	public void handleColumnDecl(String column, String identifier) {
        if(identifier == null || identifier.equals("")) {
            identifier = column;
        }
        m_colDecls.put(identifier, column);
	}
	public void handleColumnDeclStar() {}
	public void handleCondition(NodeCondition condition) {}
    
    public void handleConditionValueColumn(String column) {
        m_colCondVals.add(column);
    } 
	public void handleConditionValueLiteral(String string) {}
	public void handleConditionValueNumber(double number) {}
	public void handleCondOp(BDCondOpType eq) {}
	public void handleDelete(boolean allRows, String table) {
		m_tblAbbrs.put(table.trim(), table.trim());
	}
	public void handleInsert(String table) {
		m_tblAbbrs.put(table.trim(), table.trim());
	}
	public void handleLiteral(String m_literal) {
		
	}
	public void handleNumber(double m_number) {
		
	}
    
    public void handleOrderBy(String column, boolean asc) {}
	
    public void handleSelect() {}

	public void handleTable(String table, String identifier, BDJoinType m_jointype) {
        if(identifier == null || identifier.equals("")) {
            identifier = table;
        }
        m_tblAbbrs.put(identifier.trim(), table.trim());
	}
	
	public void handleUpdate(String table) {
		m_tblAbbrs.put(table, table);
	}

	/**
	 * Checks the column declarations against the names resolved so far, and if
	 * they are valid, puts them in the <code>m_colAbbrs</code> mapping.
	 * 
	 * @throws SemanticException
	 */
	private void resolveColumnDeclarations() throws SemanticException {
		String colName = null, tableName = null;
		boolean legalCol = false;
		
		// Check that all column names are members of a table
		for (String sColKey : m_colDecls.keySet()) {
			String colValue = m_colDecls.get(sColKey);
			
			if (colValue.contains(".")) {
				String tableAbbr = colValue.substring(0, colValue.indexOf("."));
				colName = colValue.substring(colValue.indexOf(".") + 1);

				// Ensure that columns that are qualified map to the correct
				// table.
				tableName = m_tblAbbrs.get(tableAbbr);
				if (tableName == null) {
					throw new SemanticException(tableAbbr
							+ " does not map to a valid table");
				}
				BDTable table = BDSystem.tableManager
						.getTableByName(tableName);
				if (table == null) {
					throw new SemanticException(
							"No table in database with name " + tableName);
				}
				if (table.getSchema().getNames().contains(colName)) {
					legalCol = true;
				} else {
					throw new SemanticException("No field in table "
							+ tableName + " with name " + colName);
				}
			} else {
				colName = colValue;
				for (String sTable : m_tblAbbrs.values()) {
					for (String sColInTable : BDSystem.tableManager
							.getTableByName(sTable).getSchema().getNames()) {
						if (colValue.equalsIgnoreCase(sColInTable)) {
							legalCol = true;
							tableName = sTable;
						}
					}
				}
			}

			if (!legalCol) {
				logger.error(colValue + " is not a legal column name");
				throw new SemanticException(colValue
						+ " is not a legal table name");
			} else {
            	m_colAbbrs.put(sColKey, new BDTableColumnPair(tableName, colName));
            }
        }   		
	}

	/**
	 * Resolves all of the names in the query and ensures the mappings are
	 * correct.
	 * 
	 * @return True if all the names resolve correctly, false otherwise.
	 * @throws SemanticException
	 */
	public boolean resolveNames() throws SemanticException {
        for(String s : m_tblAbbrs.values()) {
            if(!BDSystem.tableManager.isTable(s)) {
                logger.error(s + " is not a legal table name");
            	throw new SemanticException(s + " is not a legal table name");
            }
        }
        
        // Check column assignments
        for(String sCol : m_colAsgns) {
            if(sCol.contains(".")) {
                sCol = sCol.substring(sCol.indexOf(".") + 1);
            }
                    
            boolean legalCol = false;
            for (String sTableName : m_tblAbbrs.values()) {
				for (String sLegalCol : BDSystem.tableManager.getTableByName(
						sTableName).getSchema().getNames()) {
					if (sCol.equalsIgnoreCase(sLegalCol)) {
						legalCol = true;
					}
				}
            }
            if(!legalCol) {
            	logger.error(sCol + " is not a legal column name");
            	throw new SemanticException(sCol + " is not a legal table name");
            }
        }
        
        // Check column declarations in condition values
        for(String sColName : m_colCondVals) {
        	if(sColName.contains(".")) {
                sColName = sColName.substring(sColName.indexOf(".") + 1);
            }
            
            boolean legalCol = false;
			for (String sTable : m_tblAbbrs.values()) {
				for (String sColInTable : BDSystem.tableManager.getTableByName(
						sTable).getSchema().getNames()) {
					if (sColName.equalsIgnoreCase(sColInTable)) {
						legalCol = true;
					}
				}
			}
			if (!legalCol) {
				logger.error(sColName + " is not a legal column name");
				throw new SemanticException(sColName + " is not a legal column name");
            }     
        }     
        
        resolveColumnDeclarations();
        
        return true;
    }
}
