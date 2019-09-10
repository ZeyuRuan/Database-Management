package bdsim.server.exec.nodes;

import bdsim.server.exec.BDVisitor;

/**
 * A list of columns.
 * @author wpijewsk
 * @revision $Id: NodeColumnList.java 172 2006-05-09 10:26:44 +0000 (Tue, 09 May 2006) wpijewsk $
 */
public class NodeColumnList extends NodeColumnClause {

    private String m_column;
    private String m_identifier;
    private NodeColumnList m_next;

    /**
     * Class constructor.
     * 
     * @param column The name of the column
     * @param identifier The identifier for that column in the rest of the statement
     * @param next The rest of the column names
     */
    public NodeColumnList(String column, String identifier, NodeColumnList next) {
        this.m_column = column;
        this.m_identifier = identifier;
        this.m_next = next;
    }

    @Override
    public void visit(BDVisitor visitor) {
        visitor.handleColumnDecl(m_column, m_identifier);

        if (m_next != null) {
            m_next.visit(visitor);
        }
    }

    @Override
    public boolean equals(Object other) {
        if ((other == null) || !(other instanceof NodeColumnList)) {
            return false;
        }
        NodeColumnList otherList = (NodeColumnList) other;

        boolean areColumnsEqual = m_column.equals(otherList.m_column);

        boolean areIdsEqual = false;
        if (m_identifier == null) {
            areIdsEqual = (otherList.m_identifier == null);
        } else {
            areIdsEqual = m_identifier.equals(otherList.m_identifier);
        }

        boolean areNextsEqual = false;
        if (m_next == null) {
            areNextsEqual = (otherList.m_next == null);
        } else {
            areNextsEqual = m_next.equals(otherList.m_next);
        }

        return areColumnsEqual && areIdsEqual && areNextsEqual;
    }
}
