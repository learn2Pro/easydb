package org.learn2pro.easydb.storage;

import java.io.IOException;
import org.learn2pro.easydb.storage.common.Field;
import org.learn2pro.easydb.storage.common.IntField;

/**
 * The delete operator. Delete reads tuples from its child operator and removes them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    /**
     * the transaction id
     */
    private TransactionId tid;
    /**
     * the child operator
     */
    private OpIterator child;
    /**
     * the delete size
     */
    private Integer deleteSize;

    /**
     * Constructor specifying the transaction that this delete belongs to as well as the child to read from.
     *
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.deleteSize = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        while (child.hasNext()) {
            Tuple t = child.next();
            if (t != null) {
                try {
                    Database.getBufferPool().deleteTuple(tid, t);
                    deleteSize += 1;
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
     * Deletes tuples as they are read from the child operator. Deletes are processed via the buffer pool (which can be
     * accessed via the Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (deleteSize == null) {
            return null;
        } else {
            Tuple ans = new Tuple(getTupleDesc(), new Field[]{new IntField(deleteSize)});
            deleteSize = null;
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
