package org.learn2pro.easydb.storage;

import org.learn2pro.easydb.storage.IntegerAggregator.GroupIterator;
import org.learn2pro.easydb.storage.IntegerAggregator.GroupResult;
import org.learn2pro.easydb.storage.IntegerAggregator.NoGroupIterator;
import org.learn2pro.easydb.storage.IntegerAggregator.NoGroupResult;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    /**
     * the group by field index
     */
    private int groupby;
    /**
     * the group by type
     */
    private Type groupByType;
    /**
     * the aggregate field index
     */
    private int aggregate;
    /**
     * the operator
     */
    private Op op;
    /**
     * the group result
     */
    private GroupResult groupResult;
    /**
     * the no group result
     */
    private NoGroupResult noGroupResult;

    /**
     * Aggregate constructor
     *
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no
     *         grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupby = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregate = afield;
        this.op = what;
        this.noGroupResult = new NoGroupResult();
        this.groupResult = new GroupResult();
    }

    public StringAggregator(int gbfield, int afield, Op what) {
        // some code goes here
        this.groupby = gbfield;
        this.aggregate = afield;
        this.op = what;
        this.noGroupResult = new NoGroupResult();
        this.groupResult = new GroupResult();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (withOutGroup()) {
            this.op.noGroupMerge(tup.getField(aggregate), this.noGroupResult);
        } else {
            this.op.groupMerge(tup.getField(groupby), tup.getField(aggregate), this.groupResult);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a single
     *         (aggregateVal) if no grouping. The aggregateVal is determined by the type of aggregate specified in the
     *         constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (withOutGroup()) {
            return new NoGroupIterator(this.noGroupResult.getNoGroupResult(), this.noGroupResult.getNoGroupSizeResult(),
                    this.op);
        } else {
            return new GroupIterator(this.groupResult.getGroupResult(), this.groupResult.getGroupSizeResult(),
                    groupByType, this.op);
        }
    }

    @Override
    public int groupby() {
        return groupby;
    }

    @Override
    public int aggregate() {
        return aggregate;
    }

    @Override
    public Op op() {
        return op;
    }

    @Override
    public void reset() {
        this.noGroupResult = new NoGroupResult();
        this.groupResult = new GroupResult();
    }
}
