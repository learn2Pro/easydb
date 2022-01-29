package org.learn2pro.easydb.storage;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constants used for return codes in Field.compare
     */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS) {
                return "=";
            }
            if (this == GREATER_THAN) {
                return ">";
            }
            if (this == LESS_THAN) {
                return "<";
            }
            if (this == LESS_THAN_OR_EQ) {
                return "<=";
            }
            if (this == GREATER_THAN_OR_EQ) {
                return ">=";
            }
            if (this == LIKE) {
                return "LIKE";
            }
            if (this == NOT_EQUALS) {
                return "<>";
            }
            throw new IllegalStateException("impossible to reach here");
        }

    }

    /**
     * the left field
     */
    private int left;
    /**
     * the operator
     */
    private Op op;
    /**
     * the compare value
     */
    private Field value;

    /**
     * Constructor.
     *
     * @param field field number of passed in tuples to compare against.
     * @param op operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.left = field;
        this.op = op;
        this.value = operand;
    }

    /**
     * @return the field number
     */
    public int getField() {
        // some code goes here
        return left;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        // some code goes here
        return this.op;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        // some code goes here
        return this.value;
    }

    /**
     * Compares the field number of t specified in the constructor to the operand field specified in the constructor
     * using the operator specific in the constructor. The comparison can be made through Field's compare method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // some code goes here
        Field f = t.getField(this.left);
        Field right = this.value;
        return f.compare(this.op, right);
//        switch (this.op) {
//            case LIKE:
//                Preconditions.checkArgument(f instanceof StringField, "the input field:%s must be string", this.left);
//                Preconditions.checkArgument(right instanceof StringField, "the input field value:%s must be string",
//                        this.value);
//                String origin = ((StringField) f).getValue();
//                String[] parts = ((StringField) right).getValue().split("%");
//                int index = 0;
//                for (String part : parts) {
//                    if (Strings.isNullOrEmpty(part)) {
//                        continue;
//                    }
//                    index = origin.indexOf(part, index);
//                    if (index < 0) {
//                        return false;
//                    }
//                    index += part.length();
//                }
//                return true;
//            case NOT_EQUALS:
//                return !Objects.equals(f.toString(), right.toString());
//            case EQUALS:
//                return Objects.equals(f.toString(), right.toString());
//            case GREATER_THAN:
//                Preconditions.checkArgument(f instanceof IntField && right instanceof IntField,
//                        "the left and right value must be number,left:%s,right:%s", f, right);
//                return ((IntField) f).getValue() > ((IntField) right).getValue();
//            case LESS_THAN:
//                Preconditions.checkArgument(f instanceof IntField && right instanceof IntField,
//                        "the left and right value must be number,left:%s,right:%s", f, right);
//                return ((IntField) f).getValue() < ((IntField) right).getValue();
//            case LESS_THAN_OR_EQ:
//                Preconditions.checkArgument(f instanceof IntField && right instanceof IntField,
//                        "the left and right value must be number,left:%s,right:%s", f, right);
//                return ((IntField) f).getValue() <= ((IntField) right).getValue();
//            case GREATER_THAN_OR_EQ:
//                Preconditions.checkArgument(f instanceof IntField && right instanceof IntField,
//                        "the left and right value must be number,left:%s,right:%s", f, right);
//                return ((IntField) f).getValue() >= ((IntField) right).getValue();
//        }
//        return false;
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand = operand_string"
     */
    @Override
    public String toString() {
        return "Predicate{" +
                "f=" + left +
                ", op=" + op +
                ", value=" + value +
                '}';
    }
}
