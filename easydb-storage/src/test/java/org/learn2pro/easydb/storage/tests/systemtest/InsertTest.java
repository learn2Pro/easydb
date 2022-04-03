package org.learn2pro.easydb.storage.tests.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.learn2pro.easydb.storage.Database;
import org.learn2pro.easydb.storage.DbException;
import org.learn2pro.easydb.storage.HeapFile;
import org.learn2pro.easydb.storage.Insert;
import org.learn2pro.easydb.storage.SeqScan;
import org.learn2pro.easydb.storage.TransactionAbortedException;
import org.learn2pro.easydb.storage.TransactionId;
import org.learn2pro.easydb.storage.Tuple;
import org.learn2pro.easydb.storage.common.IntField;

public class InsertTest extends SimpleDbTestBase {
    private void validateInsert(int columns, int sourceRows, int destinationRows)
                throws DbException, IOException, TransactionAbortedException {
        // Create the two tables
        List<List<Integer>> sourceTuples = new ArrayList<List<Integer>>();
        HeapFile source = SystemTestUtil.createRandomHeapFile(
                columns, sourceRows, null, sourceTuples);
        assert sourceTuples.size() == sourceRows;
        List<List<Integer>> destinationTuples = new ArrayList<List<Integer>>();
        HeapFile destination = SystemTestUtil.createRandomHeapFile(
                columns, destinationRows, null, destinationTuples);
        assert destinationTuples.size() == destinationRows;

        // Insert source into destination
        TransactionId tid = new TransactionId();
        SeqScan ss = new SeqScan(tid, source.getId(), "");
        Insert insOp = new Insert(tid, ss, destination.getId());

//        Query q = new Query(insOp, tid);
        insOp.open();
        boolean hasResult = false;
        while (insOp.hasNext()) {
            Tuple tup = insOp.next();
            assertFalse(hasResult);
            hasResult = true;
            assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, tup.getTupleDesc());
            assertEquals(sourceRows, ((IntField) tup.getField(0)).getValue());
        }
        assertTrue(hasResult);
        insOp.close();

        // As part of the same transaction, scan the table
        sourceTuples.addAll(destinationTuples);
        SystemTestUtil.matchTuples(destination, tid, sourceTuples);

        // As part of a different transaction, scan the table
        Database.getBufferPool().transactionComplete(tid);
        Database.getBufferPool().flushAllPages();
        SystemTestUtil.matchTuples(destination, sourceTuples);
    }

    @Test public void testEmptyToEmpty()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(3, 0, 0);
    }

    @Test public void testEmptyToOne()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(8, 0, 1);
    }

    @Test public void testOneToEmpty()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(3, 1, 0);
    }

    @Test public void testOneToOne()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(1, 1, 1);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(InsertTest.class);
    }
}
