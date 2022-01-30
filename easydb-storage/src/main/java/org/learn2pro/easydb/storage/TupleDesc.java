package org.learn2pro.easydb.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_FIELD_NAME = "unknown";
    /**
     * the size of field definitions
     */
    private List<TDItem> struct;
    /**
     * the size of fields
     */
    private int size;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public TDItem(Type t) {
            this.fieldName = DEFAULT_FIELD_NAME;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType && Objects.equals(fieldName, tdItem.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return struct.iterator();
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the specified types, with associated named
     * fields.
     *
     * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must contain at
     *         least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        Preconditions.checkArgument(typeAr.length == fieldAr.length, "must specify the same size of data!");
        int size = typeAr.length;
        List<TDItem> struct = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            struct.add(new TDItem(typeAr[i], fieldAr[i]));
        }
        this.struct = struct;
        this.size = this.struct.size();
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified types, with anonymous
     * (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must contain at
     *         least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        int size = typeAr.length;
        List<TDItem> struct = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            struct.add(new TDItem(typeAr[i]));
        }
        this.struct = struct;
        this.size = this.struct.size();
    }

    public TupleDesc(List<TDItem> struct) {
        this.struct = struct;
        this.size = struct.size();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.size;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try {
            Preconditions.checkArgument(i >= 0 && i < numFields(), "input index is abnormal! all field size:%s",
                    numFields());
            return this.struct.get(i).fieldName;
        } catch (Exception e) {
            e.printStackTrace();
            throw new NoSuchElementException();
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try {
            Preconditions.checkArgument(i >= 0 && i < numFields(), "input index is abnormal! all field size:%s",
                    numFields());
            return this.struct.get(i).fieldType;
        } catch (Exception e) {
            e.printStackTrace();
            throw new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < numFields(); i++) {
            if (this.struct.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException(String.format("not found this field:%s in schema", name));
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note that tuples from a given TupleDesc
     *         are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int byteSize = 0;
        for (TDItem item : this.struct) {
            byteSize += item.fieldType.getLen();
        }
        return byteSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields, with the first td1.numFields coming
     * from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> merged = Lists.newArrayListWithExpectedSize(td1.getSize() + td2.getSize());
        Iterator<TDItem> fst = td1.iterator();
        Iterator<TDItem> snd = td2.iterator();
        while (fst.hasNext()) {
            merged.add(fst.next());
        }
        while (snd.hasNext()) {
            merged.add(snd.next());
        }
        return new TupleDesc(merged);
    }

    public List<TDItem> getStruct() {
        return struct;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two TupleDescs are considered equal if they have
     * the same number of items and if the i-th type in this TupleDesc is equal to the i-th type in o for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            return Objects.equals(this.struct, ((TupleDesc) o).getStruct());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(struct);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form "fieldType[0](fieldName[0]), ...,
     * fieldType[M](fieldName[M])", although the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return struct
                .stream()
                .map(item -> item.fieldType.name() + "(" + item.fieldName + ")")
                .collect(Collectors.joining(","));
    }
}
