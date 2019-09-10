package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * A SQL SELECT statement.
 * 
 * @author wpijewsk
 * @revision $Id: NodeSelect.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public class NodeSelect extends NodeStatement {

    private boolean m_all;
    private boolean m_distinct;
    private NodeColumnClause m_columns;
    private NodeTableList m_tables;
    private NodeConditionList m_conditions;
    private NodeOrderByList m_sorting;

    /**
     * Class constructor.
     * 
     * @param all Whether or not the 'ALL' flag is specified
     * @param distinct Whether or not the 'DISTINCT' flag is specified
     * @param columns The columns to select from
     * @param tables The tables to select from
     * @param conditions The conditions by which to select
     * @param sorting The sorting parameters
     */
    public NodeSelect(boolean all, boolean distinct, NodeColumnClause columns,
            NodeTableList tables, NodeConditionList conditions,
            NodeOrderByList sorting) {
        this.m_all = all;
        this.m_distinct = distinct;
        this.m_columns = columns;
        this.m_tables = tables;
        this.m_conditions = conditions;
        this.m_sorting = sorting;
    }

    @Override
    public void visit(BDVisitor visitor) {
        visitor.handleSelect();
        
        m_columns.visit(visitor);
        m_tables.visit(visitor);
        if (m_conditions != null) {
            m_conditions.visit(visitor);
        }
        if (m_sorting != null) {
            m_sorting.visit(visitor);
        }
    }

    /**
     * @return  Whether this statement has the 'ALL' flag set
     */
    public boolean isAll() {
        return m_all;
    }

    /**
     * @return Whether this statement has the 'DISTINCT' flag set
     */
    public boolean isDistinct() {
        return m_distinct;
    }

    @Override
    public boolean equals(Object other) {
        if ((other == null) || !(other instanceof NodeSelect)) {
            return false;
        }
        NodeSelect otherSelect = (NodeSelect) other;
        boolean areFlagsEqual = (m_all == otherSelect.m_all)
                && (m_distinct == otherSelect.m_distinct);
        
        boolean areColumnsEqual = m_columns.equals(otherSelect.m_columns);
        boolean areTablesEqual = m_tables.equals(otherSelect.m_tables);
        
        boolean areConditionsEqual = false;
        if(m_conditions == null) {
            areConditionsEqual = (otherSelect.m_conditions == null);
        } else {
            areConditionsEqual = m_conditions.equals(otherSelect.m_conditions);
        }
        
        boolean areSortingsEqual = false;
        if(m_sorting == null) {
            areSortingsEqual = (otherSelect.m_sorting == null);
        } else {
            areSortingsEqual = m_sorting.equals(otherSelect.m_sorting);
        }        
        
        return areFlagsEqual && areColumnsEqual && areTablesEqual && areSortingsEqual && areConditionsEqual;
    }
}
