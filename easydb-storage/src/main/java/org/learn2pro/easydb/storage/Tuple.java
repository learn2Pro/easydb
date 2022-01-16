package org.learn2pro.easydb.storage;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a specified schema specified by a TupleDesc
 * object and contain Field objects with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * the schema for each field
     */
    private TupleDesc schema;
    /**
     * the value of each field
     */
    private Field[] values;
    /**
     * the record no in page
     */
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.schema = td;
        this.values = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        this.values[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        Preconditions.checkArgument(i >= 0 && i < this.schema.numFields(), "index out of fields bound!");
        return this.values[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        return Arrays.stream(this.values)
                .map(String::valueOf)
                .collect(Collectors.joining("\t"));
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        return Arrays.stream(this.values).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.schema = td;
    }
}
