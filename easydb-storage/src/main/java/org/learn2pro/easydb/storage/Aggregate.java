package org.learn2pro.easydb.storage;

import com.google.common.collect.Lists;
import java.util.NoSuchElementException;
import org.learn2pro.easydb.storage.TupleDesc.TDItem;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only support aggregates
 * over a single column, grouped by a single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * the aggregator
     */
    private Aggregator aggregator;
    /**
     * group by type
     */
    private Type groupType;
    /**
     * aggregate type
     */
    private Type aggType;
    /**
     * the child
     */
    private OpIterator child;
    /**
     * the aggregate iterator
     */
    private OpIterator aggIterator;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to construct an {@link IntegerAggregator} or
     * {@link StringAggregator} to help you with your implementation of readNext().
     *
     * @param child The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.groupType = gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
        this.aggType = child.getTupleDesc().getFieldType(afield);
        switch (this.aggType) {
            case INT_TYPE:
                this.aggregator = new IntegerAggregator(gfield, this.groupType, afield, aop);
                break;
            case STRING_TYPE:
                this.aggregator = new StringAggregator(gfield, this.groupType, afield, aop);
                break;
        }
        this.child = child;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby field index in the <b>INPUT</b> tuples.
     *         If not, return {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.aggregator.groupby();
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name of the groupby field in the <b>OUTPUT</b>
     *         tuples. If not, return null;
     */
    public String groupFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(groupField());
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.aggregator.aggregate();
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(aggregateField());
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aggregator.op();
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        while (child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(child.next());
        }
        this.aggIterator = this.aggregator.iterator();
        this.aggIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
     * and the second field is the result of computing the aggregate. If there is no group by field, then the result
     * tuple should contain one field representing the result of the aggregate. Should return null if there are no more
     * tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (this.aggIterator.hasNext()) {
            return this.aggIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field, this will have one field - the aggregate
     * column. If there is a group by field, the first field will be the group by field, and the second will be the
     * aggregate value column.
     *
     * The name of an aggregate column should be informative. For example: "aggName(aop)
     * (child_td.getFieldName(afield))" where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (groupField() == Aggregator.NO_GROUPING) {
            return new TupleDesc(Lists.newArrayList(new TDItem(Type.INT_TYPE, aggregateFieldName())));
        } else {
            return new TupleDesc(Lists.newArrayList(new TDItem(groupType, groupFieldName()),
                    new TDItem(Type.INT_TYPE, aggregateFieldName())));
        }

    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        this.aggIterator.close();
        this.aggIterator = null;
        this.aggregator.reset();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children != null && children.length == 1) {
            this.child = children[0];
        }
    }

}
