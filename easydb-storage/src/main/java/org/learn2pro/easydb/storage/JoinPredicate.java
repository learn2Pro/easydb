package org.learn2pro.easydb.storage;

import java.io.Serializable;
import org.learn2pro.easydb.storage.Predicate.Op;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate is most likely used by the Join
 * operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * the column index of left
     */
    private int indexOfLeft;
    /**
     * the column index of right
     */
    private int indexOfRight;
    /**
     * the predicate
     */
    private Op predicate;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op The operation to apply (as defined in Predicate.Op); either Predicate.Op.GREATER_THAN,
     *         Predicate.Op.LESS_THAN, Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *         Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.indexOfLeft = field1;
        this.indexOfRight = field2;
        this.predicate = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        Field left = t1.getField(indexOfLeft);
        Field right = t2.getField(indexOfRight);
        return left.compare(predicate, right);
    }

    public int getField1() {
        // some code goes here
        return indexOfLeft;
    }

    public int getField2() {
        // some code goes here
        return indexOfRight;
    }

    public Predicate.Op getOperator() {
        // some code goes here
        return predicate;
    }
}
