package simpledb.opt;

import simpledb.parse.QueryData;
import simpledb.planner.QueryPlanner;
import simpledb.query.Plan;
import simpledb.query.ProjectPlan;
import simpledb.query.SelectPlan;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

public class SelectQueryPlanner implements QueryPlanner {
    private List<TablePlanner> tableplanners = new ArrayList<TablePlanner>();

    /**
     * Creates an optimized left-deep query plan using the following
     * heuristics.
     * H1. Choose the smallest table (considering selection predicates)
     * to be first in the join order.
     * H2. Add the table to the join order which
     * results in the smallest output.
     */
    public Plan createPlan(QueryData data, Transaction tx, int joinType) {
        // Step 1:  Create a TablePlanner object for each mentioned table
        for (String tblname : data.tables()) {
            TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, joinType);
            tableplanners.add(tp);
        }

        Plan currentplan = tableplanners.remove(0).getPlan();

        // Step 2:  Choose the lowest-size plan to begin the join order
        while (!tableplanners.isEmpty()) {
            Plan p = getLowestJoinPlan(currentplan);
            if (p != null)
                currentplan = p;
            else  // no applicable join
                currentplan = getLowestProductPlan(currentplan);
        }

        // Step 3: add the selection predicate
        currentplan = new SelectPlan(currentplan, data.pred());

        // Step 4.  Project on the field names and return
        return  new ProjectPlan(currentplan, data.fields());
    }

    private Plan getLowestSelectPlan() {
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tableplanners) {
            Plan plan = tp.makeSelectPlan();
            if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
                besttp = tp;
                bestplan = plan;
            }
        }
        tableplanners.remove(besttp);
        return bestplan;
    }

    private Plan getLowestJoinPlan(Plan current) {
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tableplanners) {
            Plan plan = tp.makeJoinPlan(current);
            if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
                besttp = tp;
                bestplan = plan;
            }
        }
        if (bestplan != null)
            tableplanners.remove(besttp);
        return bestplan;
    }

    private Plan getLowestProductPlan(Plan current) {
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tableplanners) {
            Plan plan = tp.makeProductPlan(current);
            if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
                besttp = tp;
                bestplan = plan;
            }
        }
        tableplanners.remove(besttp);
        return bestplan;
    }
}
