package org.learn2pro.easydb.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.learn2pro.easydb.storage.TupleDesc.TDItem;
import org.learn2pro.easydb.storage.common.Field;
import org.learn2pro.easydb.storage.common.IntField;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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
     * @param what the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupby = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregate = afield;
        this.op = what;
        this.groupResult = new GroupResult();
        this.noGroupResult = new NoGroupResult();
    }

    public IntegerAggregator(int gbfield, int afield, Op what) {
        // some code goes here
        this.groupby = gbfield;
        this.aggregate = afield;
        this.op = what;
        this.groupResult = new GroupResult();
        this.noGroupResult = new NoGroupResult();
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field f = tup.getField(aggregate);
        if (withOutGroup()) {
            this.op.noGroupMerge(f, this.noGroupResult);
        } else {
            Field group = tup.getField(groupby);
            this.op.groupMerge(group, f, this.groupResult);
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
        return this.groupby;
    }

    @Override
    public int aggregate() {
        return this.aggregate;
    }

    @Override
    public Op op() {
        return this.op;
    }

    @Override
    public void reset() {
        this.noGroupResult = new NoGroupResult();
        this.groupResult = new GroupResult();
    }

    static class NoGroupIterator extends Operator {

        /**
         * the result
         */
        private Integer result;
        /**
         * the size
         */
        private Integer size;
        /**
         * the operator
         */
        private Op op;

        public NoGroupIterator(Integer result, Integer size, Op op) {
            this.result = result;
            this.size = size;
            this.op = op;
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException when rewind is unsupported.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
        }

        /**
         * Returns the next Tuple in the iterator, or null if the iteration is finished. Operator uses this method to
         * implement both <code>next</code> and <code>hasNext</code>.
         *
         * @return the next Tuple in the iterator, or null if the iteration is finished.
         */
        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            if (result == null) {
                return null;
            } else {
                if (this.op == Op.AVG) {
                    Tuple t0 = new Tuple(getTupleDesc(), new Field[]{new IntField(result / size)});
                    result = null;
                    size = null;
                    return t0;
                } else {
                    Tuple t0 = new Tuple(getTupleDesc(), new Field[]{new IntField(result)});
                    result = null;
                    size = null;
                    return t0;
                }
            }
        }

        /**
         * @return return the children DbIterators of this operator. If there is only one child, return an array of only
         *         one element. For join operators, the order of the children is not important. But they should be
         *         consistent among multiple calls.
         */
        @Override
        public OpIterator[] getChildren() {
            return new OpIterator[0];
        }

        /**
         * Set the children(child) of this operator. If the operator has only one child, children[0] should be used. If
         * the operator is a join, children[0] and children[1] should be used.
         *
         * @param children the DbIterators which are to be set as the children(child) of this operator
         */
        @Override
        public void setChildren(OpIterator[] children) {

        }

        /**
         * @return return the TupleDesc of the output tuples of this operator
         */
        @Override
        public TupleDesc getTupleDesc() {
            return new TupleDesc(Lists.newArrayList(new TDItem(Type.INT_TYPE)));
        }
    }

    static class GroupIterator extends Operator {

        /**
         * the group info
         */
        private Map<Field, Integer> groups;
        /**
         * the size group
         */
        private Map<Field, Integer> sizeGroup;
        /**
         * the iterator of group
         */
        private Iterator<Entry<Field, Integer>> iterator;
        private Iterator<Entry<Field, Integer>> sizeIterator;
        /**
         * the key type
         */
        private Type type;
        /**
         * the operator
         */
        private Op op;

        public GroupIterator(Map<Field, Integer> groups,
                Map<Field, Integer> sizeGroup, Type type, Op op) {
            this.groups = groups;
            this.sizeGroup = sizeGroup;
            this.type = type;
            this.op = op;
        }

        /**
         * Closes this iterator. If overridden by a subclass, they should call super.close() in order for Operator's
         * internal state to be consistent.
         */
        @Override
        public void close() {
            super.close();
            this.iterator = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            this.iterator = groups.entrySet().iterator();
            this.sizeIterator = sizeGroup.entrySet().iterator();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException when rewind is unsupported.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        /**
         * Returns the next Tuple in the iterator, or null if the iteration is finished. Operator uses this method to
         * implement both <code>next</code> and <code>hasNext</code>.
         *
         * @return the next Tuple in the iterator, or null if the iteration is finished.
         */
        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            while (this.iterator.hasNext()) {
                Entry<Field, Integer> entry = this.iterator.next();
                Entry<Field, Integer> sizeEntry = this.sizeIterator.next();
                if (this.op == Op.AVG) {
                    return new Tuple(getTupleDesc(),
                            new Field[]{entry.getKey(), new IntField(entry.getValue() / sizeEntry.getValue())});
                } else {
                    return new Tuple(getTupleDesc(),
                            new Field[]{entry.getKey(), new IntField(entry.getValue())});
                }
            }
            return null;
        }

        /**
         * @return return the children DbIterators of this operator. If there is only one child, return an array of only
         *         one element. For join operators, the order of the children is not important. But they should be
         *         consistent among multiple calls.
         */
        @Override
        public OpIterator[] getChildren() {
            return new OpIterator[0];
        }

        /**
         * Set the children(child) of this operator. If the operator has only one child, children[0] should be used. If
         * the operator is a join, children[0] and children[1] should be used.
         *
         * @param children the DbIterators which are to be set as the children(child) of this operator
         */
        @Override
        public void setChildren(OpIterator[] children) {

        }

        /**
         * @return return the TupleDesc of the output tuples of this operator
         */
        @Override
        public TupleDesc getTupleDesc() {
            return new TupleDesc(Lists.newArrayList(new TDItem(this.type), new TDItem(Type.INT_TYPE)));
        }
    }

    static class NoGroupResult {

        /**
         * no group
         */
        private Integer noGroupResult;
        /**
         * no group size
         */
        private Integer noGroupSizeResult;

        public NoGroupResult() {
            this.noGroupResult = 0;
            this.noGroupSizeResult = 0;
        }

        public NoGroupResult(Integer noGroupResult, Integer noGroupSizeResult) {
            this.noGroupResult = noGroupResult;
            this.noGroupSizeResult = noGroupSizeResult;
        }

        public Integer getNoGroupResult() {
            return noGroupResult;
        }

        public void setNoGroupResult(Integer noGroupResult) {
            this.noGroupResult = noGroupResult;
        }

        public Integer getNoGroupSizeResult() {
            return noGroupSizeResult;
        }

        public void setNoGroupSizeResult(Integer noGroupSizeResult) {
            this.noGroupSizeResult = noGroupSizeResult;
        }

        public void addGroupSize(int i) {
            if (this.noGroupSizeResult == null) {
                this.noGroupSizeResult = i;
            } else {
                this.noGroupSizeResult += i;
            }
        }
    }

    static class GroupResult {

        /**
         * the group result
         */
        private Map<Field, Integer> groupResult = Maps.newLinkedHashMap();
        /**
         * the group size result
         */
        private Map<Field, Integer> groupSizeResult = Maps.newLinkedHashMap();

        public Map<Field, Integer> getGroupResult() {
            return groupResult;
        }

        public Map<Field, Integer> getGroupSizeResult() {
            return groupSizeResult;
        }

        public void addGroupSize(Field f, int i) {
            Integer size = groupSizeResult.get(f);
            if (size == null) {
                size = i;
            } else {
                size += i;
            }
            groupSizeResult.put(f, size);
        }
    }

}
