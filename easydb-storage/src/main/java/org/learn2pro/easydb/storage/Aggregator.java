package org.learn2pro.easydb.storage;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.learn2pro.easydb.storage.IntegerAggregator.GroupResult;
import org.learn2pro.easydb.storage.IntegerAggregator.NoGroupResult;

/**
 * The common interface for any class that can compute an aggregate over a list of Tuples.
 */
public interface Aggregator extends Serializable {

    static final int NO_GROUPING = -1;

    /**
     * SUM_COUNT and SC_AVG will only be used in lab7, you are not required to implement them until then.
     */
    public enum Op implements Serializable {
        MIN {
            @Override
            public void noGroupMerge(Field input, NoGroupResult result) {
                Preconditions.checkArgument(input instanceof IntField,
                        "not support other type of field to caclulate min!");
                result.setNoGroupResult(Math.min(result.getNoGroupResult(), ((IntField) input).getValue()));
                result.addGroupSize(1);
            }

            @Override
            public void groupMerge(Field f, Field input, GroupResult result) {
                Preconditions.checkArgument(input instanceof IntField,
                        "not support other type of field to caclulate min!");
                Integer aggregator = result.getGroupResult().get(f);
                aggregator = Math.min(aggregator == null ? Integer.MAX_VALUE : aggregator,
                        ((IntField) input).getValue());
                result.getGroupResult().put(f, aggregator);
                result.addGroupSize(f, 1);
            }
        }, MAX {
            @Override
            public void noGroupMerge(Field input, NoGroupResult result) {
                Preconditions.checkArgument(input instanceof IntField,
                        "not support other type of field to caclulate min!");
                result.setNoGroupResult(Math.max(result.getNoGroupResult(), ((IntField) input).getValue()));
                result.addGroupSize(1);
            }

            @Override
            public void groupMerge(Field f, Field input, GroupResult result) {
                Preconditions.checkArgument(input instanceof IntField,
                        "not support other type of field to caclulate min!");
                Integer aggregator = result.getGroupResult().get(f);
                aggregator = Math.max(aggregator == null ? Integer.MIN_VALUE : aggregator,
                        ((IntField) input).getValue());
                result.getGroupResult().put(f, aggregator);
                result.addGroupSize(f, 1);
            }
        }, SUM {
            @Override
            public void noGroupMerge(Field input, NoGroupResult result) {
                Preconditions.checkArgument(input instanceof IntField,
                        "not support other type of field to caclulate min!");
                result.setNoGroupResult(Integer.sum(result.getNoGroupResult(), ((IntField) input).getValue()));
                result.addGroupSize(1);
            }

            @Override
            public void groupMerge(Field f, Field input, GroupResult result) {
                Preconditions.checkArgument(input instanceof IntField,
                        "not support other type of field to caclulate min!");
                Integer aggregator = result.getGroupResult().get(f);
                aggregator = Integer.sum(aggregator == null ? 0 : aggregator, ((IntField) input).getValue());
                result.getGroupResult().put(f, aggregator);
                result.addGroupSize(f, 1);
            }
        }, AVG {
            @Override
            public void noGroupMerge(Field input, NoGroupResult result) {
                Preconditions.checkArgument(input instanceof IntField,
                        "not support other type of field to caclulate min!");
                Integer sum = Integer.sum(result.getNoGroupResult(), ((IntField) input).getValue());
                result.addGroupSize(1);
                result.setNoGroupResult(sum);
            }

            @Override
            public void groupMerge(Field f, Field input, GroupResult result) {
                Preconditions.checkArgument(input instanceof IntField,
                        "not support other type of field to caclulate min!");
                Integer aggregator = result.getGroupResult().get(f);
                aggregator = aggregator == null ? 0 : aggregator;
                Integer sum = Integer.sum(aggregator, ((IntField) input).getValue());
                result.addGroupSize(f, 1);
                result.getGroupResult().put(f, sum);
            }
        }, COUNT {
            @Override
            public void noGroupMerge(Field input, NoGroupResult result) {
                result.addGroupSize(1);
                result.setNoGroupResult(result.getNoGroupResult());
            }

            @Override
            public void groupMerge(Field f, Field input, GroupResult result) {
                result.addGroupSize(f, 1);
                result.getGroupResult().put(f, result.getGroupSizeResult().get(f));
            }
        },
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be needed to compute distributed avg in lab7.
         */
        SUM_COUNT {
            @Override
            public void noGroupMerge(Field input, NoGroupResult result) {

            }

            @Override
            public void groupMerge(Field f, Field input, GroupResult result) {

            }
        },
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples, will be used to compute distributed avg in lab7.
         */
        SC_AVG {
            @Override
            public void noGroupMerge(Field input, NoGroupResult result) {

            }

            @Override
            public void groupMerge(Field f, Field input, GroupResult result) {

            }
        };

        /**
         * Interface to access operations by a string containing an integer index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public abstract void noGroupMerge(Field input, NoGroupResult result);

        public abstract void groupMerge(Field f, Field input, GroupResult result);

        public String toString() {
            if (this == MIN) {
                return "min";
            }
            if (this == MAX) {
                return "max";
            }
            if (this == SUM) {
                return "sum";
            }
            if (this == SUM_COUNT) {
                return "sum_count";
            }
            if (this == AVG) {
                return "avg";
            }
            if (this == COUNT) {
                return "count";
            }
            if (this == SC_AVG) {
                return "sc_avg";
            }
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value; creates a new group aggregate result if the
     * group value has not yet been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @see TupleIterator for a possible helper
     */
    public OpIterator iterator();

    public int groupby();

    public int aggregate();

    public Op op();

    public void reset();

    default boolean withOutGroup() {
        return Aggregator.NO_GROUPING == groupby();
    }

}
