package simpledb;

//import java.util.HashMap;

import javafx.util.Pair;

import java.util.Map;

/**
 * A utility class, which computes the estimated cardinalities of an operator tree.
 * <p>
 * All methods have been fully provided. No extra codes are required.
 */
class OperatorCardinality {

    /**
     * @param tableAliasToId table alias to table id mapping
     * @param tableStats table statistics
     */
    static boolean updateOperatorCardinality(Operator o, Map<String, Integer> tableAliasToId,
                                             Map<String, TableStats> tableStats) {
        if (o instanceof Filter) {
            return updateFilterCardinality((Filter) o, tableAliasToId, tableStats);
        }
        else if (o instanceof Join) {
            return updateJoinCardinality((Join) o, tableAliasToId, tableStats);
        }
        else if (o instanceof HashEquiJoin) {
            return updateHashEquiJoinCardinality((HashEquiJoin) o, tableAliasToId, tableStats);
        }
        else if (o instanceof Aggregate) {
            return updateAggregateCardinality((Aggregate) o, tableAliasToId, tableStats);
        }
        else {
            DbIterator[] children = o.getChildren();
            int childC = 1;
            boolean hasJoinPK = false;
            if (children.length > 0 && children[0] != null) {
                if (children[0] instanceof Operator) {
                    hasJoinPK = updateOperatorCardinality((Operator) children[0], tableAliasToId, tableStats);
                    childC = ((Operator) children[0]).getEstimatedCardinality();
                }
                else if (children[0] instanceof SeqScan) {
                    childC = tableStats.get(((SeqScan) children[0]).getTableName()).estimateTableCardinality(1.0);
                }
            }
            o.setEstimatedCardinality(childC);
            return hasJoinPK;
        }
    }

    private static boolean updateFilterCardinality(Filter f, Map<String, Integer> tableAliasToId,
                                                   Map<String, TableStats> tableStats) {
        DbIterator child = f.getChildren()[0];
        Predicate pred = f.getPredicate();
        String[] tmp = child.getTupleDesc().getFieldName(pred.getField()).split("[.]");
        String tableAlias = tmp[0];
        String pureFieldName = tmp[1];
        Integer tableId = tableAliasToId.get(tableAlias);
        double selectivity;
        if (tableId != null) {
            selectivity = tableStats.get(Database.getCatalog().getTableName(tableId)).estimateSelectivity(
                    Database.getCatalog().getTupleDesc(tableId).fieldNameToIndex(pureFieldName), pred.getOp(),
                    pred.getOperand());
            if (child instanceof Operator) {
                Operator oChild = (Operator) child;
                boolean hasJoinPK = updateOperatorCardinality(oChild, tableAliasToId, tableStats);
                f.setEstimatedCardinality((int) (oChild.getEstimatedCardinality() * selectivity) + 1);
                return hasJoinPK;
            }
            else if (child instanceof SeqScan) {
                f.setEstimatedCardinality((int) (tableStats.get(((SeqScan) child).getTableName())
                                                           .estimateTableCardinality(1.0) * selectivity) + 1);
                return false;
            }
        }
        f.setEstimatedCardinality(1);
        return false;
    }

    private static boolean updateJoinCardinality(Join j, Map<String, Integer> tableAliasToId,
                                                 Map<String, TableStats> tableStats) {
        return updateJoinCardinalityBase(j, tableAliasToId, tableStats);
    }

    private static boolean updateHashEquiJoinCardinality(HashEquiJoin j, Map<String, Integer> tableAliasToId,
                                                         Map<String, TableStats> tableStats) {
        return updateJoinCardinalityBase(j, tableAliasToId, tableStats);
    }

    private static boolean updateAggregateCardinality(Aggregate a, Map<String, Integer> tableAliasToId,
                                                      Map<String, TableStats> tableStats) {
        DbIterator child = a.getChildren()[0];
        int childCard = 1;
        boolean hasJoinPK = false;
        if (child instanceof Operator) {
            Operator oChild = (Operator) child;
            hasJoinPK = updateOperatorCardinality(oChild, tableAliasToId, tableStats);
            childCard = oChild.getEstimatedCardinality();
        }

        if (a.groupField() == Aggregator.NO_GROUPING) {
            a.setEstimatedCardinality(1);
            return hasJoinPK;
        }

        if (child instanceof SeqScan) {
            childCard = tableStats.get(((SeqScan) child).getTableName()).estimateTableCardinality(1.0);
        }

        String[] tmp = a.groupFieldName().split("[.]");
        String tableAlias = tmp[0];
        String pureFieldName = tmp[1];
        Integer tableId = tableAliasToId.get(tableAlias);

        double groupFieldAvgSelectivity;
        if (tableId != null) {
            groupFieldAvgSelectivity = tableStats.get(Database.getCatalog().getTableName(tableId)).avgSelectivity(
                    Database.getCatalog().getTupleDesc(tableId).fieldNameToIndex(pureFieldName), Predicate.Op.EQUALS);
            a.setEstimatedCardinality((int) (Math.min(childCard, 1.0 / groupFieldAvgSelectivity)));
            return hasJoinPK;
        }
        a.setEstimatedCardinality(childCard);
        return hasJoinPK;
    }

    /**
     * Customized methods.
     */

    private static boolean updateJoinCardinalityBase(Operator j, Map<String, Integer> tableAliasToId,
                                                     Map<String, TableStats> tableStats) {
        Join jj = null;
        HashEquiJoin hj = null;
        if (j instanceof Join) {
            jj = (Join) j;
        }
        else if (j instanceof HashEquiJoin) {
            hj = (HashEquiJoin) j;
        }
        else {
            throw new IllegalArgumentException(String.format(
                    "Try to update join cardinality, but operator j is of type %s", j.toString()));
        }
        DbIterator[] children = j.getChildren();
        DbIterator child1 = children[0];
        DbIterator child2 = children[1];

        String[] tmp1 = jj != null ? jj.getJoinField1Name().split("[.]") : hj.getJoinField1Name().split("[.]");
        String tableAlias1 = tmp1[0];
        String pureFieldName1 = tmp1[1];
        String[] tmp2 = jj != null ? jj.getJoinField2Name().split("[.]") : hj.getJoinField2Name().split("[.]");
        String tableAlias2 = tmp2[0];
        String pureFieldName2 = tmp2[1];

        boolean child1HasJoinPK = Database.getCatalog().getPrimaryKey(tableAliasToId.get(tableAlias1)).equals(
                pureFieldName1);
        boolean child2HasJoinPK = Database.getCatalog().getPrimaryKey(tableAliasToId.get(tableAlias2)).equals(
                pureFieldName2);

        Pair<Boolean, Integer> child1Pair = getChildBase(child1, child1HasJoinPK, tableAliasToId, tableStats);
        child1HasJoinPK = child1Pair.getKey();
        int child1Card = child1Pair.getValue();

        Pair<Boolean, Integer> child2Pair = getChildBase(child2, child2HasJoinPK, tableAliasToId, tableStats);
        child2HasJoinPK = child2Pair.getKey();
        int child2Card = child2Pair.getValue();

        JoinPredicate joinPredicate = jj != null ? jj.getJoinPredicate() : hj.getJoinPredicate();
        j.setEstimatedCardinality(JoinOptimizer.estimateTableJoinCardinality(joinPredicate.getOperator(), tableAlias1,
                                                                             tableAlias2, pureFieldName1,
                                                                             pureFieldName2, child1Card, child2Card,
                                                                             child1HasJoinPK, child2HasJoinPK,
                                                                             tableStats, tableAliasToId));
        return child1HasJoinPK || child2HasJoinPK;
    }

    static private Pair<Boolean, Integer> getChildBase(DbIterator child, boolean childHasJoinPK,
                                                       Map<String, Integer> tableAliasToId,
                                                       Map<String, TableStats> tableStats) {
        int childCard = 1;
        if (child instanceof Operator) {
            Operator childO = (Operator) child;
            boolean pk = updateOperatorCardinality(childO, tableAliasToId, tableStats);
            childHasJoinPK = childHasJoinPK || pk;
            childCard = childO.getEstimatedCardinality();
            childCard = childCard > 0 ? childCard : 1;
        }
        else if (child instanceof SeqScan) {
            childCard = tableStats.get(((SeqScan) child).getTableName()).estimateTableCardinality(1.0);
        }
        return new Pair<>(childHasJoinPK, childCard);
    }

}
