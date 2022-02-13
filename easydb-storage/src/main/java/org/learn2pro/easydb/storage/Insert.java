package org.learn2pro.easydb.storage;

import com.google.common.collect.Lists;
import java.io.IOException;
import org.learn2pro.easydb.storage.TupleDesc.TDItem;
import org.learn2pro.easydb.storage.common.Field;
import org.learn2pro.easydb.storage.common.IntField;

/**
 * Inserts tuples read from the child operator into the tableId specified in the constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * the transaction id
     */
    private TransactionId tid;
    /**
     * the child to read tuple
     */
    private OpIterator child;
    /**
     * the table to operate
     */
    private int tableId;
    /**
     * the size for inserted tuple
     */
    private Integer insertSize;

    /**
     * Constructor.
     *
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.insertSize = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(Lists.newArrayList(new TDItem(Type.INT_TYPE)));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        while (child.hasNext()) {
            Tuple t = child.next();
            if (t != null) {
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, t);
                    insertSize += 1;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the constructor. It returns a one field tuple
     * containing the number of inserted records. Inserts should be passed through BufferPool. An instances of
     * BufferPool is available via Database.getBufferPool(). Note that insert DOES NOT need check to see if a particular
     * tuple is a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (insertSize == null) {
            return null;
        } else {
            TupleDesc td = getTupleDesc();
            Tuple ans = new Tuple(td, new Field[]{new IntField(insertSize)});
            insertSize = null;
            return ans;
        }
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
