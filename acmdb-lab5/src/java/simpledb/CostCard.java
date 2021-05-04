package simpledb;

import java.util.Vector;

/**
 * Class returned by {@link JoinOptimizer#computeCostAndCardOfSubplan} specifying the cost and cardinality of the
 * optimal plan represented by plan.
 */
public class CostCard {
    /**
     * The cost of the optimal subplan
     */
    double cost;
    /**
     * The cardinality of the optimal subplan
     */
    int card;
    /**
     * The optimal subplan
     */
    Vector<LogicalJoinNode> plan;
}
